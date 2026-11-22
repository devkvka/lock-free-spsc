# Lock free SPSC

A generic lock-free SPSC queue implemented in (Odin)[https://odin-lang.org].

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
