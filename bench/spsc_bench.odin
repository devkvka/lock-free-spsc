package bench

import "core:log"
import "base:intrinsics"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"
import spsc ".."

ITEMS :: 50_000_000
RUNS  :: 5

run :: proc(t: ^testing.T, $Size: uint) {
    q := new(spsc.SPSC_Queue(Size, u64))
    defer free(q)

    best := max(time.Duration)
    bad: u64

    for _ in 0..<RUNS {
        q^ = {}
        go: bool
        errors: u64

        prod := thread.create_and_start_with_poly_data3(q, u64(ITEMS), &go,
            proc(q: ^spsc.SPSC_Queue(Size, u64), n: u64, go: ^bool) {
                for !sync.atomic_load_explicit(go, .Acquire) do intrinsics.cpu_relax()
                for i in 0..<n {
                    for !spsc.spsc_write(q, i) do intrinsics.cpu_relax()
                }
            })

        cons := thread.create_and_start_with_poly_data4(q, u64(ITEMS), &go, &errors,
            proc(q: ^spsc.SPSC_Queue(Size, u64), n: u64, go: ^bool, errors: ^u64) {
                for !sync.atomic_load_explicit(go, .Acquire) do intrinsics.cpu_relax()
                for expected in 0..<n {
                    v, ok := spsc.spsc_read(q)
                    for !ok {
                        intrinsics.cpu_relax()
                        v, ok = spsc.spsc_read(q)
                    }
                    if v != expected do errors^ += 1
                }
            })

        start := time.tick_now()
        sync.atomic_store_explicit(&go, true, .Release)
        thread.join(prod)
        thread.join(cons)
        elapsed := time.tick_since(start)

        thread.destroy(prod)
        thread.destroy(cons)

        best = min(best, elapsed)
        bad += errors
    }

    ns := f64(time.duration_nanoseconds(best)) / ITEMS
    log.infof("size %6d: %6.2f ns/op  %7.2f Mops/s", Size, ns, 1000 / ns)
    testing.expect_value(t, bad, 0)
}

@(test) bench_16    :: proc(t: ^testing.T) { run(t, 16) }
@(test) bench_32    :: proc(t: ^testing.T) { run(t, 32) }
@(test) bench_64    :: proc(t: ^testing.T) { run(t, 64) }
@(test) bench_1024  :: proc(t: ^testing.T) { run(t, 1024) }
@(test) bench_65536 :: proc(t: ^testing.T) { run(t, 65536) }
@(test) bench_131072 :: proc(t: ^testing.T) { run(t, 131072) }
