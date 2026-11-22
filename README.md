# Lock free SPSC

A generic lock-free SPSC queue implemented in [Odin](https://odin-lang.org).
The implementation used no AI, but tests and benchmarks were made by claude.
I made this because I needed a lock-free queue for a project.
I read some existing resources (mostly [rigtorp's writings](https://github.com/rigtorp/SPSCQueue)) 
then implemented this with the knowledge gained from those.

## Results

Tested on a MacBook Air M3
A queue of size 1, acting as the sanity check results in around 80 ns/op, touching
each others caches at every operation. 
Above this, it quickly drops steadily an order of magnitude down to <3 ns/op
between queue lengths 128 and 1024. I hypothesise this to be because the cursors
almost never need to touch each other's caches and also the entire structure fits in L1.
Above this, due to reasons unknown to me, the queue gets noticeably slower at 5-9 ns/op
at 2048-4096. This could be caused by many things, for example because other work might be 
running on the core so the entire data might not fit in L1 cache reliably. I do not know 
this for sure though. Above 8192 there is a steep curve in performance to almost 40 ns/op 
and stays very close to this up to 131k elements. 

## Testing

To run tests:
```
odin test . -define:ODIN_TEST_THREADS=1
odin test . -define:ODIN_TEST_THREADS=1 -o:speed
odin test . -define:ODIN_TEST_THREADS=1 -sanitize:thread
```

To run benchmarks:
```
odin test bench -o:speed -define:ODIN_TEST_THREADS=1
```


