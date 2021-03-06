/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Scheduler;

import static reactor.core.scheduler.Scheduler.REJECTED;

final class FluxCancelOn<T> extends FluxSource<T, T> {

	final Scheduler scheduler;

	public FluxCancelOn(Flux<T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new CancelSubscriber<T>(s, scheduler));
	}

	static final class CancelSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final Subscriber<? super T> actual;
		final Scheduler             scheduler;

		Subscription s;

		volatile int cancelled = 0;
		static final AtomicIntegerFieldUpdater<CancelSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(CancelSubscriber.class, "cancelled");

		CancelSubscriber(Subscriber<? super T> actual, Scheduler scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return s;
				case CANCELLED:
					return cancelled == 1;
			}
			return InnerOperator.super.scan(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void run() {
			s.cancel();
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				if (scheduler.schedule(this) == REJECTED) {
					//TODO should this really throw onRejectedExecution?
					throw Operators.onRejectedExecution();
				}
			}
		}
	}
}
