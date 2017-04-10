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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.concurrent.QueueSupplier;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing publish-subscribe with
 * synchronous (thread-stealing and happen-before interactions) drain loops.
 * <p>
 *     The default {@link #create} factories will only produce the new elements observed in
 *     the
 *     parent sequence after a given {@link Subscriber} is subscribed.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/emitter.png" alt="">
 * <p>
 *
 * @author Stephane Maldini
 *
 * @param <T> the input and output value type
 */
public final class EmitterProcessor<T> extends FluxProcessor<T, T> {

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create() {
		return create(true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
     * @param autoCancel automatically cancel
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(boolean autoCancel) {
		return create(QueueSupplier.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
     * @param bufferSize the internal buffer size to hold signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize) {
		return create(bufferSize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
     * @param bufferSize the internal buffer size to hold signals
     * @param concurrency the concurrency level of the emission
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, int concurrency) {
		return create(bufferSize, concurrency, true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
     * @param bufferSize the internal buffer size to hold signals
     * @param autoCancel automatically cancel
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, boolean autoCancel) {
		return create(bufferSize, Integer.MAX_VALUE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @param bufferSize the internal buffer size to hold signals
	 * @param concurrency the concurrency level of the emission
	 * @param autoCancel automatically cancel
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, bufferSize);
	}

	final int             maxConcurrency;
	final int             bufferSize;
	final int             limit;
	final boolean         autoCancel;
	volatile Subscription s;


	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class, Subscription.class, "s");

	volatile RingBuffer<EventLoopProcessor.Slot<T>> emitBuffer;

	volatile boolean done;

	static final EmitterInner<?>[] EMPTY = new EmitterInner<?>[0];

	static final EmitterInner<?>[] CANCELLED = new EmitterInner<?>[0];

	volatile Throwable error;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
	Throwable.class, "error");

	volatile EmitterInner<?>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, EmitterInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
					EmitterInner[].class,
					"subscribers");
	@SuppressWarnings("unused")
	volatile int wip;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> WIP =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "wip");

	volatile int outstanding;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> OUTSTANDING =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "outstanding");
	boolean firstDrain = true;

	EmitterProcessor(boolean autoCancel, int maxConcurrency, int bufferSize) {
		if (bufferSize < 1){
			throw new IllegalArgumentException("bufferSize must be strictly positive, " +
					"was: "+bufferSize);
		}
		this.autoCancel = autoCancel;
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
		this.limit = Math.max(1, bufferSize / 2);
		OUTSTANDING.lazySet(this, bufferSize);
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		EmitterInner<T> inner = new EmitterInner<>(this, s);
		if(addInner(inner)) {
			inner.init();
			s.onSubscribe(inner);
		}
		else{
			Operators.complete(inner.actual);
		}
	}

	@Override
	public EmitterProcessor<T> connect() {
		onSubscribe(Operators.emptySubscription());
		return this;
	}

	/**
	 * Return the number of parked elements in the emitter backlog.
	 *
	 * @return the number of parked elements in the emitter backlog.
	 */
	public long getPending() {
		return (emitBuffer == null ? -1L :
				emitBuffer.getPending());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(T t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}

		EmitterInner<?>[] inner = subscribers;
		if (inner == CANCELLED) {
			return;
		}

		int n = inner.length;
		if (n != 0) {

			long seq = -1L;

			int outstanding;
			if (s != null) {

				outstanding = this.outstanding;
				if (outstanding != 0) {
					OUTSTANDING.decrementAndGet(this);
				}
				else {
					buffer(t);
					drain();
					return;
				}
			}

			for (int i = 0; i < n; i++) {

				EmitterInner<T> is = (EmitterInner<T>) inner[i];

				if (is.done) {
					removeInner(is, autoCancel ? CANCELLED : EMPTY);

					if (autoCancel && subscribers == CANCELLED) {
						if (WIP.compareAndSet(this, 0, 1)) {
							Operators.terminate(S, this);
						}
						return;
					}
					continue;
				}
					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE;
					RingBuffer.Sequence poll = is.unbounded ? null : is.pollCursor;

					//no tracking and remaining demand positive
					if (r > 0L && poll == null) {
						if (r != Long.MAX_VALUE) {
							EmitterInner.REQUESTED.decrementAndGet(is);
						}
						is.actual.onNext(t);
					}
					//if failed, we buffer if not previously buffered and we assign a tracking cursor to that slot
					else {
						if (seq == -1L) {
							seq = buffer(t);
							startAllTrackers(inner, seq, i + 1);
						}
						else if(poll == null){
							is.startTracking(seq);
						}
					}

			}

			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			drainLoop();

		}
		else {
			buffer(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		if (done) {
			Operators.onErrorDropped(t);
		}
		reportError(t);
		done = true;
		drain();
	}

	@Override
	public void onComplete() {
		if (done) {
			return;
		}
		done = true;
		drain();
	}

	@Override
	public void onSubscribe(final Subscription s) {
		if (Operators.setOnce(S, this, s)) {
			drain();
		}
	}

	@Override
	public Throwable getError() {
		return error;
	}

	/**
	 * Return whether has all subcribers cancelled and {@link #create(boolean)} auto
	 * shutdown argument
	 * been set to true.
	 *
	 * @return whether has all subcribers cancelled and {@link #create(boolean)} auto
	 * shutdown argument been set to true.
	 */
	public boolean isCancelled() {
		return subscribers == CANCELLED;
	}

	@Override
	final public int getBufferSize() {
		return bufferSize;
	}

	@Override
	public boolean isTerminated() {
		return done && (emitBuffer == null || emitBuffer.getPending() == 0);
	}

	@Override
	public Object scan(Attr key) {
		switch (key) {
			case PARENT:
				return s;
			case BUFFERED:
				return emitBuffer == null ? 0 : emitBuffer.getPending();
			case CANCELLED:
				return isCancelled();
		}
		return super.scan(key);
	}

	RingBuffer<EventLoopProcessor.Slot<T>> getMainQueue() {
		RingBuffer<EventLoopProcessor.Slot<T>> q = emitBuffer;
		if (q == null) {
			q = EventLoopProcessor.createSingleProducer(bufferSize);
			emitBuffer = q;
		}
		return q;
	}

	final long buffer(T value) {
		RingBuffer<EventLoopProcessor.Slot<T>> q = getMainQueue();

		long seq = q.next();

		q.get(seq).value = value;
		q.publish(seq);
		return seq;
	}

	final void drain() {
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	final void drainLoop() {
		int missed = 1;
		RingBuffer<EventLoopProcessor.Slot<T>> q = null;
		for (; ; ) {
			EmitterInner<?>[] inner = subscribers;
			if (inner == CANCELLED) {
				Operators.terminate(S, this);
				return;
			}
			boolean d = done;

			if (d && inner == EMPTY) {
				return;
			}

			int n = inner.length;

			if (n != 0) {
				RingBuffer.Sequence innerSequence;
				long _r;

				for (int i = 0; i < n; i++) {
					@SuppressWarnings("unchecked") EmitterInner<T> is = (EmitterInner<T>) inner[i];

					long r = is.requested;

					if (is.done) {
						removeInner(is, autoCancel ? CANCELLED : EMPTY);
						if (autoCancel && subscribers == CANCELLED) {
							Operators.terminate(S, this);
							return;
						}
						continue;
					}

					if (q == null) {
						q = emitBuffer;
					}
					innerSequence = is.pollCursor;
					if (!is.unbounded && innerSequence != null && r > 0) {
						_r = r;

						boolean unbounded = _r == Long.MAX_VALUE;
						T oo;

						long cursor = innerSequence.getAsLong();
						long max = q.getCursor();
						while (_r != 0L) {
							cursor++;
							if (max >= cursor) {
								oo = q.get(cursor).value;
							}
							else {
								break;
							}

							innerSequence.set(cursor);
							is.actual.onNext(oo);

							if (!unbounded) {
								_r--;
							}
						}

						if (!unbounded && r > _r) {
							EmitterInner.REQUESTED.addAndGet(is, _r - r);
						}

					}
					else {
						_r = 0L;
					}

					if (d) {
						checkTerminal(is, innerSequence, _r);
					}
				}

				if (!done && firstDrain) {
					Subscription s = this. s;
					if (s != null) {
						firstDrain = false;
						s.request(bufferSize);
					}
				}
				else {
					if (q != null) {
						requestMore(q.getPending());
					}
					else {
						requestMore(0);
					}
				}
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	final void checkTerminal(EmitterInner<T> is, RingBuffer.Sequence innerSequence, long r) {
		Throwable e = error;
		if ((e != null && r == 0) || innerSequence == null || is.unbounded || innerSequence.getAsLong() >= emitBuffer.getCursor()) {
			removeInner(is, EMPTY);
			if (!is.done) {
				if (e == null) {
					is.actual.onComplete();
				}
				else {
					is.actual.onError(e);
				}
			}
		}
	}

	final void startAllTrackers(EmitterInner<?>[] inner, long seq, int size) {
		RingBuffer.Sequence poll;
		for (int i = 0; i < size - 1; i++) {
			poll = inner[i].pollCursor;
			if (poll == null) {
				inner[i].startTracking(seq + 1);
			}
		}
		inner[size - 1].startTracking(seq);
	}

	final void reportError(Throwable t) {
		ERROR.compareAndSet(this, null, t);
	}

	final boolean addInner(EmitterInner<T> inner) {
		for (; ; ) {
			EmitterInner<?>[] a = subscribers;
			if (a == CANCELLED) {
				return false;
			}
			int n = a.length;
			if (n + 1 > maxConcurrency) {
				throw Exceptions.failWithOverflow();
			}
			EmitterInner<?>[] b = new EmitterInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = inner;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	final void removeInner(EmitterInner<?> inner, EmitterInner<?>[] lastRemoved) {
		for (; ; ) {
			EmitterInner<?>[] a = subscribers;
			if (a == CANCELLED || a == EMPTY) {
				return;
			}
			int n = a.length;
			int j = 0;
			for (int i = 0; i < n; i++) {
				if (a[i] == inner) {
					j = i;
					break;
				}
			}
			EmitterInner<?>[] b;
			if (n == 1) {
				b = lastRemoved;
			}
			else {
				b = new EmitterInner<?>[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				RingBuffer.Sequence poll = inner.pollCursor;
				if (poll != null) {
					getMainQueue().removeGatingSequence(poll);
				}
				return;
			}
		}
	}

	final void requestMore(int buffered) {

		if (buffered < bufferSize) {
			int r = outstanding;
			if (r > limit) {
				return;
			}
			int toRequest = (bufferSize - r) - buffered;

			Subscription subscription = s;
			if (toRequest > 0 && subscription != null) {
				OUTSTANDING.addAndGet(this, toRequest);
				subscription.request(toRequest);
			}
		}
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	@Override
	public String toString() {
		return "{" +
				"done: " + done +
				(error != null ? ", error: '" + error.getMessage() + "', " : "") +
				", outstanding: " + outstanding +
				", pending: " + emitBuffer +
				'}';
	}

	static final class EmitterInner<T>
			implements InnerProducer<T> {

		final EmitterProcessor<T>   parent;
		final Subscriber<? super T> actual;

		volatile boolean done;

		boolean unbounded = false;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<EmitterInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(EmitterInner.class, "requested");

		volatile RingBuffer.Sequence pollCursor;

		@SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<EmitterInner, RingBuffer.Sequence> CURSOR =
				AtomicReferenceFieldUpdater.newUpdater(EmitterInner.class,
						RingBuffer.Sequence.class,
						"pollCursor");

		EmitterInner(EmitterProcessor<T> parent, final Subscriber<? super T> actual) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.checkRequest(n, actual)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				if (EmitterProcessor.WIP.getAndIncrement(parent) == 0) {
					parent.drainLoop();
				}
			}
		}

		@Override
		public void cancel() {
			done = true;
			parent.drain();
		}

		@Override
		public final Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return parent;
				case TERMINATED:
				case CANCELLED:
					return done;
				case BUFFERED:
					return pollCursor == null || done ? -1L : parent.emitBuffer.getCursor() - pollCursor.getAsLong();
			}
			return InnerProducer.super.scan(key);
		}

		void startTracking(long seq) {
			RingBuffer.Sequence pollSequence = RingBuffer.newSequence(seq - 1L);
			if (CURSOR.compareAndSet(this, null, pollSequence)) {
				parent.emitBuffer.addGatingSequence(pollSequence);
			}
		}

		void init() {
			RingBuffer<EventLoopProcessor.Slot<T>> ringBuffer = parent.emitBuffer;
			if (ringBuffer != null) {
				startTracking(Math.max(0L, ringBuffer.getMinimumGatingSequence() + 1L));
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}
	}


}
