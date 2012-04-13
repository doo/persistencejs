/**
 * Back-end using minimal WinRT SQLite3 bindings
 */

function log(o) {
  console.log(o);
}

var persistence = (window && window.persistence) ? window.persistence : {}; 

if(!persistence.store) {
  persistence.store = {};
}

persistence.store.sqlite3_winrt = {};

persistence.store.sqlite3_winrt.config = function(persistence, dbPath) {
  var db = new SQLite3JS.Database(dbPath);

  persistence.transaction = function (explicitCommit, fn) {
    if (typeof arguments[0] === "function") {
      fn = arguments[0];
      explicitCommit = false;
    }
    var tx = _transaction(db);
    if (explicitCommit) {
      tx.executeSql("START TRANSACTION", null, function(){
        fn(tx)
      });
    }
    else 
      fn(tx);
  };

  persistence.close = function() {
    db.close();
  };

  function _transaction(db){
    var that = {};
    // TODO: add check for db opened or closed
    that.executeSql = function(query, args, successFn, errorFn){
      function cb(err, result){
        if (err) {
          log(err.message);
          that.errorHandler && that.errorHandler(err);
          errorFn && errorFn(null, err);
          return;
        }
        if (successFn) {
          successFn(result);
        }
      }
      if (persistence.debug) {
        console.log(query);
        args && args.length > 0 && console.log(args.join(", "))
      }
      var rows = db.execute(query, args);
      if (successFn)
        successFn(rows);
    }

    that.commit = function(session, callback){
      session.flush(that, function(){
        that.executeSql("COMMIT", null, callback);
      })
    }

    that.rollback = function(session, callback){
      that.executeSql("ROLLBACK", null, function() {
        session.clean();
        callback();
      });
    }
    return that;
  }

  ///////////////////////// SQLite dialect

  persistence.sqliteDialect = {
    // columns is an array of arrays, e.g.
    // [["id", "VARCHAR(32)", "PRIMARY KEY"], ["name", "TEXT"]]
    createTable: function(tableName, columns, constraints) {
      constraints = constraints || [];
      var tm = persistence.typeMapper;
      var sql = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (";
      var defs = [];
      for(var i = 0; i < columns.length; i++) {
        var column = columns[i];
        defs.push("`" + column[0] + "` " + tm.columnType(column[1]) + (column[2] ? " " + column[2] : ""));
      }
      sql += defs.concat(constraints).join(", ");
      sql += ')';
      return sql;
    },

    // columns is array of column names, e.g.
    // ["id"]
    createIndex: function(tableName, columns, options) {
      options = options || {};
      return "CREATE "+(options.unique?"UNIQUE ":"")+"INDEX IF NOT EXISTS `" + tableName + "__" + columns.join("_") + 
             "` ON `" + tableName + "` (" + 
             columns.map(function(col) { return "`" + col + "`"; }).join(", ") + ")";
    }
  };

  persistence.store.sql.config(persistence, persistence.sqliteDialect);
};

