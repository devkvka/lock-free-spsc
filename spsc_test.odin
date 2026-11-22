package spsc

import "base:intrinsics"
import "core:testing"
import "core:thread"
import "core:time"

STREAM_TOTAL :: u64(1 << 20)
TEST_TIMEOUT :: 120 * time.Second

Payload :: struct {
	seq, a, b, c, d, e, f, g: u64,
}

Wide :: struct {
	id:      int,
	tag:     string,
	weights: [3]f32,
	flags:   bit_set[0 ..< 8; u8],
}

Color :: enum u8 {
	Red,
	Green,
	Blue,
}

Xorshift :: struct {
	state: u64,
}

xorshift_next :: proc(x: ^Xorshift) -> u64 {
	s := x.state
	s ~= s << 13
	s ~= s >> 7
	s ~= s << 17
	x.state = s
	return s
}

make_payload :: proc(seq: u64) -> Payload {
	return {
		seq,
		seq * 3,
		seq * 5,
		seq * 7,
		~seq,
		seq ~ 0xA5A5A5A5A5A5A5A5,
		seq << 1,
		seq + 0xDEADBEEF,
	}
}

check_empty :: proc(t: ^testing.T, q: ^SPSC_Queue($N, $T)) {
	v, ok := spsc_read(q)
	testing.expect(t, !ok, "read on empty queue must fail")
	testing.expect(t, v == T{}, "failed read must return the zero value")
}

check_fill_and_drain :: proc(t: ^testing.T, q: ^SPSC_Queue($N, int)) {
	for round in 0 ..< 5 {
		base := round * 100_000
		for i in 0 ..< int(N) {
			testing.expectf(t, spsc_write(q, base + i), "write %d of %d must succeed", i, int(N))
		}
		testing.expect(t, !spsc_write(q, -1), "write on full queue must fail")
		testing.expect(t, !spsc_write(q, -2), "repeated write on full queue must keep failing")
		for i in 0 ..< int(N) {
			v, ok := spsc_read(q)
			testing.expectf(t, ok, "read %d must succeed", i)
			testing.expect_value(t, v, base + i)
		}
		check_empty(t, q)
	}
}

check_wraparound :: proc(t: ^testing.T, q: ^SPSC_Queue($N, int)) {
	next_write, next_read := 0, 0
	for resident in 0 ..< int(N) {
		for _ in 0 ..< resident {
			testing.expect(t, spsc_write(q, next_write))
			next_write += 1
		}
		for _ in 0 ..< int(N) * 64 + 5 {
			testing.expect(t, spsc_write(q, next_write))
			next_write += 1
			v, ok := spsc_read(q)
			testing.expect(t, ok)
			testing.expect_value(t, v, next_read)
			next_read += 1
		}
		for _ in 0 ..< resident {
			v, ok := spsc_read(q)
			testing.expect(t, ok)
			testing.expect_value(t, v, next_read)
			next_read += 1
		}
		check_empty(t, q)
	}
}

check_against_model :: proc(t: ^testing.T, q: ^SPSC_Queue($N, int), ops: int, seed: u64) {
	model := make([dynamic]int, 0, int(N))
	defer delete(model)

	rng := Xorshift{seed}
	write_bias: u64 = 50
	next_value := 1

	for op in 0 ..< ops {
		if op % 997 == 0 {
			write_bias = xorshift_next(&rng) % 101
		}
		if xorshift_next(&rng) % 100 < write_bias {
			ok := spsc_write(q, next_value)
			if !testing.expect_value(t, ok, len(model) < int(N)) {
				return
			}
			if ok {
				append(&model, next_value)
				next_value += 1
			}
		} else {
			v, ok := spsc_read(q)
			if !testing.expect_value(t, ok, len(model) > 0) {
				return
			}
			if ok {
				want := model[0]
				ordered_remove(&model, 0)
				if !testing.expect_value(t, v, want) {
					return
				}
			}
		}
	}
}

check_roundtrip :: proc(t: ^testing.T, $T: typeid, values: [4]T) {
	q: SPSC_Queue(4, T)
	check_empty(t, &q)
	for v in values {
		testing.expect(t, spsc_write(&q, v))
	}
	for want in values {
		got, ok := spsc_read(&q)
		testing.expect(t, ok)
		testing.expect(t, got == want, "value corrupted in transit")
	}
	check_empty(t, &q)
}

run_stream :: proc(
	t: ^testing.T,
	q: ^SPSC_Queue($N, Payload),
	total: u64,
	producer_yield_every: u64,
	consumer_yield_every: u64,
) {
	Ctx :: struct {
		q:                    ^SPSC_Queue(N, Payload),
		total:                u64,
		producer_yield_every: u64,
		consumer_yield_every: u64,
		errors:               u64,
		first_bad:            u64,
	}

	producer :: proc(c: ^Ctx) {
		for i in 0 ..< c.total {
			p := make_payload(i)
			for !spsc_write(c.q, p) {
				intrinsics.cpu_relax()
			}
			if c.producer_yield_every != 0 && i % c.producer_yield_every == 0 {
				thread.yield()
			}
		}
	}

	consumer :: proc(c: ^Ctx) {
		for expected in 0 ..< c.total {
			p: Payload
			ok: bool
			for {
				p, ok = spsc_read(c.q)
				if ok {
					break
				}
				intrinsics.cpu_relax()
			}
			if p != make_payload(expected) {
				if c.errors == 0 {
					c.first_bad = expected
				}
				c.errors += 1
			}
			if c.consumer_yield_every != 0 && expected % c.consumer_yield_every == 0 {
				thread.yield()
			}
		}
	}

	ctx := Ctx {
		q                    = q,
		total                = total,
		producer_yield_every = producer_yield_every,
		consumer_yield_every = consumer_yield_every,
	}

	p := thread.create_and_start_with_poly_data(&ctx, producer)
	c := thread.create_and_start_with_poly_data(&ctx, consumer)
	thread.join_multiple(p, c)
	thread.destroy(p)
	thread.destroy(c)

	testing.expectf(
		t,
		ctx.errors == 0,
		"%d corrupted, lost, duplicated or reordered items, first at sequence %d",
		ctx.errors,
		ctx.first_bad,
	)
	check_empty(t, q)
}

@(test)
test_new_queue_is_empty :: proc(t: ^testing.T) {
	q1: SPSC_Queue(1, int)
	q2: SPSC_Queue(2, int)
	q8: SPSC_Queue(8, Payload)
	q1024: SPSC_Queue(1024, Wide)
	check_empty(t, &q1)
	check_empty(t, &q2)
	check_empty(t, &q8)
	check_empty(t, &q1024)
}

@(test)
test_single_write_then_read :: proc(t: ^testing.T) {
	q: SPSC_Queue(8, int)
	testing.expect(t, spsc_write(&q, 42))
	v, ok := spsc_read(&q)
	testing.expect(t, ok)
	testing.expect_value(t, v, 42)
	check_empty(t, &q)
}

@(test)
test_repeated_empty_reads_do_not_corrupt_state :: proc(t: ^testing.T) {
	q: SPSC_Queue(4, int)
	for _ in 0 ..< 1000 {
		check_empty(t, &q)
	}
	testing.expect(t, spsc_write(&q, 7))
	v, ok := spsc_read(&q)
	testing.expect(t, ok)
	testing.expect_value(t, v, 7)
}

@(test)
test_repeated_full_writes_do_not_corrupt_state :: proc(t: ^testing.T) {
	q: SPSC_Queue(4, int)
	for i in 0 ..< 4 {
		testing.expect(t, spsc_write(&q, i))
	}
	for _ in 0 ..< 1000 {
		testing.expect(t, !spsc_write(&q, -1))
	}
	for i in 0 ..< 4 {
		v, ok := spsc_read(&q)
		testing.expect(t, ok)
		testing.expect_value(t, v, i)
	}
	check_empty(t, &q)
}

@(test)
test_fifo_order :: proc(t: ^testing.T) {
	q: SPSC_Queue(16, int)
	for i in 0 ..< 16 {
		testing.expect(t, spsc_write(&q, i * i))
	}
	for i in 0 ..< 16 {
		v, ok := spsc_read(&q)
		testing.expect(t, ok)
		testing.expect_value(t, v, i * i)
	}
}

@(test)
test_capacity_is_exactly_n :: proc(t: ^testing.T) {
	check_capacity :: proc(t: ^testing.T, q: ^SPSC_Queue($N, int)) {
		accepted := 0
		for spsc_write(q, accepted) {
			accepted += 1
			if accepted > int(N) {
				break
			}
		}
		testing.expect_value(t, accepted, int(N))
	}

	q1: SPSC_Queue(1, int)
	q2: SPSC_Queue(2, int)
	q4: SPSC_Queue(4, int)
	q64: SPSC_Queue(64, int)
	q4096: SPSC_Queue(4096, int)
	check_capacity(t, &q1)
	check_capacity(t, &q2)
	check_capacity(t, &q4)
	check_capacity(t, &q64)
	check_capacity(t, &q4096)
}

@(test)
test_full_write_does_not_overwrite :: proc(t: ^testing.T) {
	q: SPSC_Queue(4, int)
	for i in 0 ..< 4 {
		testing.expect(t, spsc_write(&q, i + 1))
	}
	testing.expect(t, !spsc_write(&q, 999))
	for i in 0 ..< 4 {
		v, ok := spsc_read(&q)
		testing.expect(t, ok)
		testing.expect_value(t, v, i + 1)
	}
}

@(test)
test_read_from_full_frees_exactly_one_slot :: proc(t: ^testing.T) {
	q: SPSC_Queue(4, int)
	for i in 0 ..< 4 {
		testing.expect(t, spsc_write(&q, i))
	}
	v, ok := spsc_read(&q)
	testing.expect(t, ok)
	testing.expect_value(t, v, 0)
	testing.expect(t, spsc_write(&q, 4))
	testing.expect(t, !spsc_write(&q, 5))
	for i in 1 ..= 4 {
		v, ok = spsc_read(&q)
		testing.expect(t, ok)
		testing.expect_value(t, v, i)
	}
	check_empty(t, &q)
}

@(test)
test_size_one_queue :: proc(t: ^testing.T) {
	q: SPSC_Queue(1, int)
	for i in 0 ..< 10_000 {
		check_empty(t, &q)
		testing.expect(t, spsc_write(&q, i))
		testing.expect(t, !spsc_write(&q, -1))
		v, ok := spsc_read(&q)
		testing.expect(t, ok)
		testing.expect_value(t, v, i)
	}
}

@(test)
test_fill_and_drain_across_sizes :: proc(t: ^testing.T) {
	q1: SPSC_Queue(1, int)
	q2: SPSC_Queue(2, int)
	q4: SPSC_Queue(4, int)
	q8: SPSC_Queue(8, int)
	q64: SPSC_Queue(64, int)
	q1024: SPSC_Queue(1024, int)
	check_fill_and_drain(t, &q1)
	check_fill_and_drain(t, &q2)
	check_fill_and_drain(t, &q4)
	check_fill_and_drain(t, &q8)
	check_fill_and_drain(t, &q64)
	check_fill_and_drain(t, &q1024)
}

@(test)
test_wraparound_across_sizes :: proc(t: ^testing.T) {
	q1: SPSC_Queue(1, int)
	q2: SPSC_Queue(2, int)
	q4: SPSC_Queue(4, int)
	q8: SPSC_Queue(8, int)
	q64: SPSC_Queue(64, int)
	check_wraparound(t, &q1)
	check_wraparound(t, &q2)
	check_wraparound(t, &q4)
	check_wraparound(t, &q8)
	check_wraparound(t, &q64)
}

@(test)
test_long_running_single_thread_alternation :: proc(t: ^testing.T) {
	q: SPSC_Queue(8, u64)
	for i in 0 ..< u64(5_000_000) {
		testing.expect(t, spsc_write(&q, i))
		v, ok := spsc_read(&q)
		testing.expect(t, ok)
		testing.expect_value(t, v, i)
	}
	check_empty(t, &q)
}

@(test)
test_randomized_ops_match_reference_model :: proc(t: ^testing.T) {
	q1: SPSC_Queue(1, int)
	q2: SPSC_Queue(2, int)
	q8: SPSC_Queue(8, int)
	q64: SPSC_Queue(64, int)
	q512: SPSC_Queue(512, int)
	check_against_model(t, &q1, 500_000, 0x9E3779B97F4A7C15)
	check_against_model(t, &q2, 500_000, 0xD1B54A32D192ED03)
	check_against_model(t, &q8, 1_000_000, 0x8CB92BA72F3D8DD7)
	check_against_model(t, &q64, 1_000_000, 0xC2B2AE3D27D4EB4F)
	check_against_model(t, &q512, 1_000_000, 0x165667B19E3779F9)
}

@(test)
test_value_types_roundtrip :: proc(t: ^testing.T) {
	check_roundtrip(t, u8, [4]u8{0, 1, 127, 255})
	check_roundtrip(t, i64, [4]i64{min(i64), -1, 0, max(i64)})
	check_roundtrip(t, f64, [4]f64{0, -0.5, 1e300, 3.14159})
	check_roundtrip(t, Color, [4]Color{.Red, .Green, .Blue, .Red})
	check_roundtrip(t, string, [4]string{"", "a", "hello", "wörld"})
	check_roundtrip(t, [64]u8, [4][64]u8{{0 ..< 64 = 1}, {0 ..< 64 = 2}, {0 ..< 64 = 0xFF}, {}})
	check_roundtrip(
		t,
		Wide,
		[4]Wide {
			{id = 1, tag = "one", weights = {1, 2, 3}, flags = {0, 7}},
			{id = 2, tag = "two", weights = {-1, 0, 1}, flags = {}},
			{id = -3, tag = "", weights = {0.5, 0.25, 0.125}, flags = {1, 2, 3}},
			{},
		},
	)
	check_roundtrip(t, Payload, [4]Payload{make_payload(0), make_payload(1), make_payload(2), make_payload(u64(max(u32)))})

	a, b, c, d := 10, 20, 30, 40
	check_roundtrip(t, ^int, [4]^int{&a, &b, &c, nil})
	_ = d
}

@(test)
test_queue_is_independent_per_instance :: proc(t: ^testing.T) {
	a: SPSC_Queue(4, int)
	b: SPSC_Queue(4, int)
	testing.expect(t, spsc_write(&a, 1))
	testing.expect(t, spsc_write(&a, 2))
	check_empty(t, &b)
	testing.expect(t, spsc_write(&b, 100))
	v, ok := spsc_read(&a)
	testing.expect(t, ok)
	testing.expect_value(t, v, 1)
	v, ok = spsc_read(&b)
	testing.expect(t, ok)
	testing.expect_value(t, v, 100)
	v, ok = spsc_read(&a)
	testing.expect(t, ok)
	testing.expect_value(t, v, 2)
}

@(test)
test_heap_allocated_queue :: proc(t: ^testing.T) {
	q := new(SPSC_Queue(256, Payload))
	defer free(q)
	for i in 0 ..< u64(256) {
		testing.expect(t, spsc_write(q, make_payload(i)))
	}
	testing.expect(t, !spsc_write(q, Payload{}))
	for i in 0 ..< u64(256) {
		v, ok := spsc_read(q)
		testing.expect(t, ok)
		testing.expect(t, v == make_payload(i))
	}
	check_empty(t, q)
}

@(test)
test_concurrent_stream_size_1 :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(1, Payload)
	run_stream(t, &q, STREAM_TOTAL / 4, 0, 0)
}

@(test)
test_concurrent_stream_size_2 :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(2, Payload)
	run_stream(t, &q, STREAM_TOTAL, 0, 0)
}

@(test)
test_concurrent_stream_size_8 :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(8, Payload)
	run_stream(t, &q, STREAM_TOTAL, 0, 0)
}

@(test)
test_concurrent_stream_size_1024 :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(1024, Payload)
	run_stream(t, &q, STREAM_TOTAL, 0, 0)
}

@(test)
test_concurrent_stream_size_65536 :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q := new(SPSC_Queue(65536, Payload))
	defer free(q)
	run_stream(t, q, STREAM_TOTAL, 0, 0)
}

@(test)
test_concurrent_slow_consumer_keeps_queue_full :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(16, Payload)
	run_stream(t, &q, STREAM_TOTAL / 8, 0, 3)
}

@(test)
test_concurrent_slow_producer_keeps_queue_empty :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(16, Payload)
	run_stream(t, &q, STREAM_TOTAL / 8, 3, 0)
}

@(test)
test_concurrent_both_sides_yielding :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	q: SPSC_Queue(4, Payload)
	run_stream(t, &q, STREAM_TOTAL / 8, 5, 7)
}

@(test)
test_concurrent_consumer_starts_before_producer :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)

	Ctx :: struct {
		q:      ^SPSC_Queue(32, u64),
		total:  u64,
		errors: u64,
	}

	producer :: proc(c: ^Ctx) {
		time.sleep(20 * time.Millisecond)
		for i in 0 ..< c.total {
			for !spsc_write(c.q, i) {
				intrinsics.cpu_relax()
			}
		}
	}

	consumer :: proc(c: ^Ctx) {
		empty_reads: u64
		for expected in 0 ..< c.total {
			v: u64
			ok: bool
			for {
				v, ok = spsc_read(c.q)
				if ok {
					break
				}
				empty_reads += 1
				intrinsics.cpu_relax()
			}
			if v != expected {
				c.errors += 1
			}
		}
		if empty_reads == 0 {
			c.errors += 1
		}
	}

	q: SPSC_Queue(32, u64)
	ctx := Ctx {
		q     = &q,
		total = 100_000,
	}
	c := thread.create_and_start_with_poly_data(&ctx, consumer)
	p := thread.create_and_start_with_poly_data(&ctx, producer)
	thread.join_multiple(p, c)
	thread.destroy(p)
	thread.destroy(c)
	testing.expect_value(t, ctx.errors, u64(0))
}

@(test)
test_concurrent_repeated_short_lifetimes :: proc(t: ^testing.T) {
	testing.set_fail_timeout(t, TEST_TIMEOUT)
	for round in 0 ..< 200 {
		q: SPSC_Queue(4, Payload)
		run_stream(t, &q, 2_000, 0, 0)
		if testing.failed(t) {
			return
		}
		_ = round
	}
}
