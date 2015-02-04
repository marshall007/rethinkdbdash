var EventEmitter = require('events').EventEmitter;
var Writable = require('stream').Writable;
var util = require('util');

function WriteStream(table, options) {
    this._table = table;
    this._batch = [];

    Writable.call(this, {
        objectMode: true,
        highWaterMark: options.batch || 128
    });

    delete options.batch;
    this.opts = options;
};

util.inherits(WriteStream, Writable);


// We still potentially have work to do after all data has been
// consumed from the incoming stream, so we have to monkey patch
// the `emit` method. See:
// https://github.com/joyent/node/issues/7348#issuecomment-62506833

WriteStream.prototype.emit = function(event) {
    var self = this;
    var emit = EventEmitter.prototype.emit;

    if (event === 'finish' && self._flush && !Writable.prototype._flush) {
        self._flush(function (err) {
            if (err) {
                emit.call(self, 'error', err);
            } else {
                emit.call(self, 'finish');
            }
        });
    } else {
        emit.apply(self, arguments);
    }
}


WriteStream.prototype._write = function(chunk, encoding, next) {
    this._batch.push(chunk);
    if (this._batch.length >= this.highWaterMark) {
        return this._flush(next);
    }
    return next();
}


WriteStream.prototype._flush = function(next) {
    var self = this;

    self.table.insert(self._batch, self.opts).run(function (err, data) {
        self._batch = [];
        next(err);
    });
}

module.exports = WriteStream;
