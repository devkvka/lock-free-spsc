package spsc

import "core:sync"

Queue_Cursor :: struct #align(128) {
    loc: uint,
    cached_other: uint,
}

// Size must be a power of 2. Just round up from whatever size you need.
SPSC_Queue :: struct($Size: uint, $T: typeid) #align(128)
    where (Size > 0) && (Size & (Size-1) == 0)
{
    w: Queue_Cursor,
    r: Queue_Cursor,
    data: [Size]T,
}

spsc_write :: proc(q: ^SPSC_Queue($N, $T), item: T) -> bool {
    if q.w.loc - q.w.cached_other == N {
        q.w.cached_other = sync.atomic_load_explicit(&q.r.loc, .Acquire)
        if q.w.loc - q.w.cached_other == N do return false
    }
    q.data[q.w.loc & (N-1)] = item
    sync.atomic_store_explicit(&q.w.loc, q.w.loc+1, .Release)
    return true
}

spsc_read :: proc(q: ^SPSC_Queue($N, $T)) -> (T, bool) {
    if q.r.loc == q.r.cached_other {
        q.r.cached_other = sync.atomic_load_explicit(&q.w.loc, .Acquire)
        if q.r.loc == q.r.cached_other do return {}, false
    }
    item := q.data[q.r.loc & (N-1)]
    sync.atomic_store_explicit(&q.r.loc, q.r.loc+1, .Release)
    return item, true
}

