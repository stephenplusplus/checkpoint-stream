'use strict'

var assert = require('assert')
var async = require('async')
var through = require('through2')

var checkpointStream = require('./index.js')

describe('checkpoint-stream', function () {
  var OBJ_WITHOUT_CHECKPOINT = {}
  var OBJ_WITH_CHECKPOINT = {}

  it('should queue events until a checkpoint is reached', function (done) {
    var source = through.obj()

    var checkpoint = checkpointStream.obj(function (obj) {
      return obj === OBJ_WITH_CHECKPOINT
    })

    var results = []
    var finalResults = through.obj(function (chunk, enc, next) {
      results.push(chunk)
      next(null, chunk)
    })

    source.pipe(checkpoint).pipe(finalResults)

    async.series([
      function (next) {
        source.push(OBJ_WITHOUT_CHECKPOINT)
        next()
      },
      function (next) {
        assert.strictEqual(checkpoint.queue.length, 1)
        next()
      },
      function (next) {
        source.push(OBJ_WITH_CHECKPOINT)
        next()
      },
      function (next) {
        assert.strictEqual(checkpoint.queue.length, 2)
        next()
      }
    ], function () {
      assert.strictEqual(results.length, 0)

      source.end()

      setImmediate(function () {
        assert.strictEqual(results.length, 2)
        done()
      })
    })
  })

  it('should release events when a limit is hit', function (done) {
    var DEFAULT_MAX_QUEUED = 10

    var source = through.obj()

    var checkpoint = checkpointStream.obj(function (obj) {
      return obj === OBJ_WITH_CHECKPOINT
    })

    var results = []
    var finalResults = through.obj(function (chunk, enc, next) {
      results.push(chunk)
      next(null, chunk)
    })

    source.pipe(checkpoint).pipe(finalResults)

    var numObjectsToPush = DEFAULT_MAX_QUEUED
    while (numObjectsToPush--) source.push(OBJ_WITHOUT_CHECKPOINT)
    source.push(OBJ_WITHOUT_CHECKPOINT) // one more to break the limit

    setTimeout(function () {
      assert.strictEqual(results.length, DEFAULT_MAX_QUEUED + 1)
      done()
    }, 10)
  })

  it('should release the queue before a failure', function (done) {
    var error = new Error(':(')

    var source = through.obj()

    var checkpoint = checkpointStream.obj(function (obj) {
      return obj === OBJ_WITH_CHECKPOINT
    })
    checkpoint.on('error', function (err) {
      assert.strictEqual(err, error)
      assert.strictEqual(results.length, 3)
      done()
    })

    var results = []
    var finalResults = through.obj(function (chunk, enc, next) {
      results.push(chunk)
      next(null, chunk)
    })

    source.pipe(checkpoint).pipe(finalResults)

    source.push(OBJ_WITHOUT_CHECKPOINT)
    source.push(OBJ_WITHOUT_CHECKPOINT)
    source.push(OBJ_WITHOUT_CHECKPOINT)

    checkpoint.destroy(error)
  })

  it('should expose a flush method', function (done) {
    var source = through.obj()

    var checkpoint = checkpointStream.obj(function (obj) {
      return obj === OBJ_WITH_CHECKPOINT
    })

    var results = []
    var finalResults = through.obj(function (chunk, enc, next) {
      results.push(chunk)
      next(null, chunk)
    })

    source.pipe(checkpoint).pipe(finalResults)

    async.series([
      function (next) {
        source.push(OBJ_WITHOUT_CHECKPOINT)
        next()
      },
      function (next) {
        assert.strictEqual(checkpoint.queue.length, 1)
        next()
      },
      function (next) {
        source.push(OBJ_WITHOUT_CHECKPOINT)
        next()
      },
      function (next) {
        assert.strictEqual(checkpoint.queue.length, 2)
        next()
      },
      function (next) {
        checkpoint.flush(function () {
          next()
        })
      }
    ], function () {
      assert.strictEqual(checkpoint.queue.length, 0)

      source.end()

      setImmediate(function () {
        assert.strictEqual(results.length, 2)
        done()
      })
    })
  })

  it('should expose a reset method', function (done) {
    var source = through.obj()

    var checkpoint = checkpointStream.obj(function (obj) {
      return obj === OBJ_WITH_CHECKPOINT
    })

    var results = []
    var finalResults = through.obj(function (chunk, enc, next) {
      results.push(chunk)
      next(null, chunk)
    })

    source.pipe(checkpoint).pipe(finalResults)

    async.series([
      function (next) {
        source.push(OBJ_WITHOUT_CHECKPOINT)
        next()
      },
      function (next) {
        assert.strictEqual(checkpoint.queue.length, 1)
        next()
      },
      function (next) {
        source.push(OBJ_WITHOUT_CHECKPOINT)
        next()
      },
      function (next) {
        assert.strictEqual(checkpoint.queue.length, 2)
        next()
      },
      function (next) {
        checkpoint.reset()
        next()
      }
    ], function () {
      assert.strictEqual(checkpoint.queue.length, 0)

      source.end()

      setImmediate(function () {
        assert.strictEqual(results.length, 0)
        done()
      })
    })
  })

  it('should emit checkpoint event', function (done) {
    var source = through.obj()

    var checkpoint = checkpointStream.obj(function (obj) {
      return obj === OBJ_WITH_CHECKPOINT
    })

    checkpoint.on('checkpoint', function (data) {
      assert.strictEqual(data, OBJ_WITH_CHECKPOINT)
      done()
    })

    source.pipe(checkpoint)
    source.push(OBJ_WITH_CHECKPOINT)
    source.end()
  })

  describe('cfg.maxQueued', function () {
    it('should allow a custom limit', function (done) {
      var MAX_QUEUED = 5
      var source = through.obj()

      var checkpoint = checkpointStream.obj({
        isCheckpointFn: function (obj) {
          return obj === OBJ_WITH_CHECKPOINT
        },
        maxQueued: MAX_QUEUED
      })

      var results = []
      var finalResults = through.obj(function (chunk, enc, next) {
        results.push(chunk)
        next(null, chunk)
      })

      source.pipe(checkpoint).pipe(finalResults)

      var numObjectsToPush = MAX_QUEUED
      while (numObjectsToPush--) source.push(OBJ_WITHOUT_CHECKPOINT)
      source.push(OBJ_WITHOUT_CHECKPOINT) // one more to break the limit

      setTimeout(function () {
        assert.strictEqual(results.length, MAX_QUEUED + 1)
        done()
      }, 10)
    })
  })
})
