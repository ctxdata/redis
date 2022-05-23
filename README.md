This README is just a fast *quick start* document. You can find more detailed documentation at [redis.io](https://redis.io).

What is Redis?
--------------

Forward to [Original Redis README](./README_redis.md)

This is a fork repository of `Redis` for experiment purpose. I'll add some features just for fun. :)

### What I have done in this fork?
1. Add `multby` command to `String` category and it's used to multiply an integer value by a given number. (Similar to `incrby`) 
2. Add a custom module `FunDB bloomfilter` working like `RedisBloom`, it supports `FunBF.add`, `FunBF.exists` and `FunBF.dump` commands for `bloomfilter` usage.

[My introduction about multby implementation](https://ctxdata.github.io/all-about-redis/start.html)

**NOTE**
All codes I added are for experiment and academic research, please don't use them in your production environment. :)
