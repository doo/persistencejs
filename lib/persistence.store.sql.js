/**
 * Default type mapper. Override to support more types or type options.
 */
var defaultTypeMapper = {
  /**
   * SQL type for ids
   */
  idType: "VARCHAR(32)",

  /**
   * SQL type for class names (used by mixins)
   */
  classNameType: "TEXT",

  /**
   * Returns SQL type for column definition
   */
  columnType: function(type){
    switch(type) {
    case 'JSON': return 'TEXT';
    case 'BOOL': return 'INT';
    case 'DATE': return 'INT';
    default: return type;
    }
  },

  inVar: function(str, type){
    return str;
  },
  outVar: function(str, type){
    return str;
  },
  outId: function(str){
    return "'" + str + "'";
  },
  /**
   * Converts a value from the database to a value suitable for the entity
   * (also does type conversions, if necessary)
   */
  dbValToEntityVal: function(val, type){
    if (val === null || val === undefined) {
      return val;
    }
    switch (type) {
      case 'DATE':
        // SQL is in seconds and JS in miliseconds
        if (val > 1000000000000) {
          // usually in seconds, but sometimes it's milliseconds
          return new Date(parseInt(val, 10));
        } else {
          return new Date(parseInt(val, 10) * 1000);
        }
      case 'BOOL':
        return val === 1 || val === '1';
        break;
      case 'INT':
        return +val;
        break;
      case 'BIGINT':
        return +val;
        break;
      case 'JSON':
        if (val) {
          return JSON.parse(val);
        }
        else {
          return val;
        }
        break;
      default:
        return val;
    }
  },

  /**
   * Converts an entity value to a database value, inverse of
   *   dbValToEntityVal
   */
  entityValToDbVal: function(val, type){
    if (val === undefined || val === null) {
      return null;
    }
    else if (type === 'JSON' && val) {
      return JSON.stringify(val);
    }
    else if (val.id) {
      return val.id;
    }
    else if (type === 'BOOL') {
      return (val === 'false') ? 0 : (val ? 1 : 0);
    }
    else if (type === 'DATE' || val.getTime) {
      // In order to make SQLite Date/Time functions work we should store
      // values in seconds and not as miliseconds as JS Date.getTime()
      val = new Date(val);
      return Math.round(val.getTime() / 1000);
    }
    else {
      return val;
    }
  },
  /**
   * Shortcut for inVar when type is id -- no need to override
   */
  inIdVar: function(str){
    return this.inVar(str, this.idType);
  },
  /**
   * Shortcut for outVar when type is id -- no need to override
   */
  outIdVar: function(str){
    return this.outVar(str, this.idType);
  },
  /**
   * Shortcut for entityValToDbVal when type is id -- no need to override
   */
  entityIdToDbId: function(id){
    return this.entityValToDbVal(id, this.idType);
  }
}

function config(persistence, dialect) {
  var argspec = persistence.argspec;

  persistence.typeMapper = dialect.typeMapper || defaultTypeMapper;

  persistence.generatedTables = {}; // set

  /**
   * Synchronize the data model with the database, creates table that had not
   * been defined before
   *
   * @param tx
   *            transaction object to use (optional)
   * @param callback
   *            function to be called when synchronization has completed,
   *            takes started transaction as argument
   */
  persistence.schemaSync = function (tx, callback, emulate) {
    var args = argspec.getArgs(arguments, [
        { name: "tx", optional: true, check: persistence.isTransaction, defaultValue: null },
        { name: "callback", optional: true, check: argspec.isCallback(), defaultValue: function(){} },
        { name: "emulate", optional: true, check: argspec.hasType('boolean') }
      ]);
    tx = args.tx;
    callback = args.callback;
    emulate = args.emulate;

    if(!tx) {
      var session = this;
      this.transaction(function(tx) { session.schemaSync(tx, callback, emulate); });
      return;
    }
    var queries = [], meta, colDefs, otherMeta, tableName;

    var tm = persistence.typeMapper;
    var entityMeta = persistence.getEntityMeta();
    for (var entityName in entityMeta) {
      if (entityMeta.hasOwnProperty(entityName)) {
        meta = entityMeta[entityName];
        if (!meta.isMixin) {
          colDefs = [];
          for (var prop in meta.fields) {
            if (meta.fields.hasOwnProperty(prop)) {
              colDefs.push([prop, meta.fields[prop]]);
            }
          }
          for (var rel in meta.hasOne) {
            if (meta.hasOne.hasOwnProperty(rel)) {
              otherMeta = meta.hasOne[rel].type.meta;
              colDefs.push([rel, tm.idType]);
              queries.push([dialect.createIndex(meta.name, [rel]), null]);
            }
          }
          for (var i = 0; i < meta.indexes.length; i++) {
            queries.push([dialect.createIndex(meta.name, meta.indexes[i].columns, meta.indexes[i]), null]);
          }
        }
        for (var rel in meta.hasMany) {
          if (meta.hasMany.hasOwnProperty(rel) && meta.hasMany[rel].manyToMany) {
            tableName = meta.hasMany[rel].tableName;
            if (!persistence.generatedTables[tableName]) {
              var otherMeta = meta.hasMany[rel].type.meta;
              var inv = meta.hasMany[rel].inverseProperty;
              // following test ensures that mixin mtm tables get created with the mixin itself
              // it seems superfluous because mixin will be processed before entitites that use it
              // but better be safe than sorry.
              if (otherMeta.hasMany[inv].type.meta != meta)
                continue;
              var p1 = meta.name + "_" + rel;
              var p2 = otherMeta.name + "_" + inv;
              queries.push([dialect.createIndex(tableName, [p1]), null]);
              queries.push([dialect.createIndex(tableName, [p2]), null]);
              var columns = [[p1, tm.idType], [p2, tm.idType]];
              if (meta.isMixin)
                columns.push([p1 + "_class", tm.classNameType])
              if (otherMeta.isMixin)
                columns.push([p2 + "_class", tm.classNameType])
              queries.push([dialect.createTable(tableName, columns, ["UNIQUE (" + p1 + ", " + p2 +")"]), null]);
              persistence.generatedTables[tableName] = true;
            }
          }
        }
        if (!meta.isMixin) {
          colDefs.push(["id", tm.idType, "PRIMARY KEY"]);
          persistence.generatedTables[meta.name] = true;
          queries.push([dialect.createTable(meta.name, colDefs), null]);
        }
      }
    }
    var fns = persistence.schemaSyncHooks;
    for(var i = 0; i < fns.length; i++) {
      fns[i](tx);
    }
    if(emulate) {
      // Done
      callback(tx);
    } else {
      executeQueriesSeq(tx, queries, function(_, err) {
          callback(tx, err);
        });
    }
  };

  /**
   * Persists all changes to the database transaction
   *
   * @param tx
   *            transaction to use
   * @param callback
   *            function to be called when done
   */
  persistence.flush = function (tx, callback) {
    var args = argspec.getArgs(arguments, [
        { name: "tx", optional: true, check: persistence.isTransaction },
        { name: "callback", optional: true, check: argspec.isCallback(), defaultValue: null }
      ]);
    tx = args.tx;
    callback = args.callback;

    var session = this;
    if(!tx) {
      this.transaction(function(tx) { session.flush(tx, callback); });
      return;
    }
    var fns = persistence.flushHooks;
    persistence.asyncForEach(fns, function(fn, callback) {
        fn(session, tx, callback);
      }, function() {
        // After applying the hooks
        var persistObjArray = [];
        for (var id in session.trackedObjects) {
          if (session.trackedObjects.hasOwnProperty(id)) {
            persistObjArray.push(session.trackedObjects[id]);
          }
        }
        var removeObjArray = [];
        for (var id in session.objectsToRemove) {
          if (session.objectsToRemove.hasOwnProperty(id)) {
            removeObjArray.push(session.objectsToRemove[id]);
            delete session.trackedObjects[id]; // Stop tracking
          }
        }
        session.objectsToRemove = {};
        if(callback) {
          persistence.asyncParForEach(removeObjArray, function(obj, callback) {
              remove(obj, tx, callback);
            }, function(result, err) {
              if (err) return callback(result, err);
              persistence.asyncParForEach(persistObjArray, function(obj, callback) {
                  save(obj, tx, callback);
                }, callback);
            });
        } else { // More efficient
          for(var i = 0; i < persistObjArray.length; i++) {
            save(persistObjArray[i], tx);
          }
          for(var i = 0; i < removeObjArray.length; i++) {
            remove(removeObjArray[i], tx);
          }
        }
      });
  };

  /**
   * Remove all tables in the database (as defined by the model)
   */
  persistence.reset = function (tx, callback) {
    var args = argspec.getArgs(arguments, [
        { name: "tx", optional: true, check: persistence.isTransaction, defaultValue: null },
        { name: "callback", optional: true, check: argspec.isCallback(), defaultValue: function(){} }
      ]);
    tx = args.tx;
    callback = args.callback;

    var session = this;
    if(!tx) {
      session.transaction(function(tx) { session.reset(tx, callback); });
      return;
    }
    // First emulate syncing the schema (to know which tables were created)
    this.schemaSync(tx, function() {
        var tableArray = [];
        for (var p in persistence.generatedTables) {
          if (persistence.generatedTables.hasOwnProperty(p)) {
            tableArray.push(p);
          }
        }
        function dropOneTable () {
          var tableName = tableArray.pop();
          tx.executeSql("DROP TABLE IF EXISTS `" + tableName + "`", null, function () {
              if (tableArray.length > 0) {
                dropOneTable();
              } else {
                cb();
              }
            }, cb);
        }
        if(tableArray.length > 0) {
          dropOneTable();
        } else {
          cb();
        }

        function cb(result, err) {
          session.clean();
          persistence.generatedTables = {};
          if (callback) callback(result, err);
        }
      }, true);
  };

  /**
   * Converts a database row into an entity object
   */
  function rowToEntity(session, entityName, row, prefix) {
    prefix = prefix || '';
    if (session.trackedObjects[row[prefix + "id"]]) { // Cached version
      return session.trackedObjects[row[prefix + "id"]];
    }
    var tm = persistence.typeMapper;
    var rowMeta = persistence.getMeta(entityName);
    var ent = persistence.define(entityName); // Get entity
    if(!row[prefix+'id']) { // null value, no entity found
      return null;
    }
    var o = new ent(session, undefined, true);
    o.id = tm.dbValToEntityVal(row[prefix + 'id'], tm.idType);
    o._new = false;
    for ( var p in row) {
      if (row.hasOwnProperty(p)) {
        if (p.substring(0, prefix.length) === prefix) {
          var prop = p.substring(prefix.length);
          if (prop != 'id') {
            o._data[prop] = tm.dbValToEntityVal(row[p], rowMeta.fields[prop] || tm.idType);
          }
        }
      }
    }
    return o;
  }

  /**
   * Internal function to persist an object to the database
   * this function is invoked by persistence.flush()
   */
  function save(obj, tx, callback) {
    var meta = persistence.getMeta(obj._type);
    var tm = persistence.typeMapper;
    var properties = [];
    var values = [];
    var qs = [];
    var propertyPairs = [];
    if(obj._new) { // Mark all properties dirty
      for (var p in meta.fields) {
        if(meta.fields.hasOwnProperty(p)) {
          obj._dirtyProperties[p] = true;
        }
      }
    }
    for ( var p in obj._dirtyProperties) {
      if (obj._dirtyProperties.hasOwnProperty(p)) {
        properties.push("`" + p + "`");
        var type = meta.fields[p] || tm.idType;
        values.push(tm.entityValToDbVal(obj._data[p], type));
        qs.push(tm.outVar("?", type));
        propertyPairs.push("`" + p + "` = " + tm.outVar("?", type));
      }
    }
    var additionalQueries = [];
    for(var p in meta.hasMany) {
      if(meta.hasMany.hasOwnProperty(p)) {
        additionalQueries = additionalQueries.concat(persistence.get(obj, p).persistQueries());
      }
    }
    executeQueriesSeq(tx, additionalQueries, function() {
        if (!obj._new && properties.length === 0) { // Nothing changed and not new
          if(callback) callback();
          return;
        }
        obj._dirtyProperties = {};
        if (obj._new) {
          properties.push('id');
          values.push(tm.entityIdToDbId(obj.id));
          qs.push(tm.outIdVar('?'));
          var sql = "INSERT INTO `" + obj._type + "` (" + properties.join(", ") + ") VALUES (" + qs.join(', ') + ")";
          obj._new = false;
          tx.executeSql(sql, values, callback, callback);
        } else {
          var sql = "UPDATE `" + obj._type + "` SET " + propertyPairs.join(',') + " WHERE id = " + tm.outId(obj.id);
          tx.executeSql(sql, values, callback, callback);
        }
      });
  }

  persistence.save = save;

  function remove (obj, tx, callback) {
    var meta = persistence.getMeta(obj._type);
    var tm = persistence.typeMapper;
    var queries = [["DELETE FROM `" + obj._type + "` WHERE id = " + tm.outId(obj.id), null]];
    for (var rel in meta.hasMany) {
      if (meta.hasMany.hasOwnProperty(rel) && meta.hasMany[rel].manyToMany) {
        var tableName = meta.hasMany[rel].tableName;
        //var inverseProperty = meta.hasMany[rel].inverseProperty;
        queries.push(["DELETE FROM `" + tableName + "` WHERE `" + meta.name + '_' + rel + "` = " + tm.outId(obj.id), null]);
      }
    }
    executeQueriesSeq(tx, queries, callback);
  }

  /**
   * Utility function to execute a series of queries in an asynchronous way
   * @param tx the transaction to execute the queries on
   * @param queries an array of [query, args] tuples
   * @param callback the function to call when all queries have been executed
   */
  function executeQueriesSeq (tx, queries, callback) {
    // queries.reverse();
    var callbackArgs = [];
    for ( var i = 3; i < arguments.length; i++) {
      callbackArgs.push(arguments[i]);
    }
    persistence.asyncForEach(queries, function(queryTuple, callback) {
        tx.executeSql(queryTuple[0], queryTuple[1], callback, function(_, err) {
            console.log(err.message);
            callback(_, err);
          });
      }, function(result, err) {
        if (err && callback) {
          callback(result, err);
          return;
        }
        if(callback) callback.apply(null, callbackArgs);
      });
  }

  persistence.executeQueriesSeq = executeQueriesSeq;

  persistence.NullFilter.prototype.sql = function (meta, alias, values) {
    return "1=1";
  };

  persistence.AndFilter.prototype.sql = function (meta, alias, values) {
    return "(" + this.left.sql(meta, alias, values) + " AND "
    + this.right.sql(meta, alias, values) + ")";
  };

  persistence.OrFilter.prototype.sql = function (meta, alias, values) {
    return "(" + this.left.sql(meta, alias, values) + " OR "
    + this.right.sql(meta, alias, values) + ")";
  };

  persistence.PropertyFilter.prototype.sql = function (meta, alias, values) {
    var tm = persistence.typeMapper;
    var aliasPrefix = alias ? "`" + alias + "`." : "";
    var sqlType = meta.fields[this.property] || tm.idType;
    if (this.operator === '=' && this.value === null) {
      return aliasPrefix + '`' + this.property + "` IS NULL";
    } else if (this.operator === '!=' && this.value === null) {
      return aliasPrefix + '`' + this.property + "` IS NOT NULL";
    } else if (this.operator === 'in') {
      var vals = this.value;
      var qs = [];
      for(var i = 0; i < vals.length; i++) {
        qs.push('?');
        values.push(tm.entityValToDbVal(vals[i], sqlType));
      }
      if(vals.length === 0) {
        // Optimize this a little
        return "1 = 0";
      } else {
        return aliasPrefix + '`' + this.property + "` IN (" + qs.join(', ') + ")";
      }
    } else if (this.operator === 'not in') {
      var vals = this.value;
      var qs = [];
      for(var i = 0; i < vals.length; i++) {
        qs.push('?');
        values.push(tm.entityValToDbVal(vals[i], sqlType));
      }

      if(vals.length === 0) {
        // Optimize this a little
        return "1 = 1";
      } else {
        return aliasPrefix + '`' + this.property + "` NOT IN (" + qs.join(', ') + ")";
      }
    } else if (this.operator === 'like') {
      var escapedValue = this.value.replace(/([\\%_])/g, '\\$1');
      values.push(tm.entityValToDbVal("%" + escapedValue + "%", sqlType));
      return aliasPrefix + '`' + this.property + "` LIKE " + tm.outVar("?", sqlType) + " ESCAPE '\\'";
    } else {
      var value = this.value;
      if(value === true || value === false) {
        value = value ? 1 : 0;
      }
      values.push(tm.entityValToDbVal(value, sqlType));
      return aliasPrefix + '`' + this.property + "` " + this.operator + " " + tm.outVar("?", sqlType);
    }
  };

  persistence.DbQueryCollection.prototype.fromSql = function (mainAlias) {
    var sql = " FROM `" + this._entityName + "`";
    if (mainAlias) {
      sql += " AS `" + mainAlias + "`" 
    }
    return sql;
  }

  persistence.DbQueryCollection.prototype.joinSql = function (mainAlias) {
    return "";
  }

  persistence.DbQueryCollection.prototype.whereSql = function (mainAlias, args) {
    var meta = persistence.getMeta(this._entityName);
    return " WHERE " + this._filter.sql(meta, mainAlias, args);
  }

  /**
   * Asynchronous call to actually fetch the items in the collection
   * @param tx transaction to use
   * @param callback function to be called taking an array with
   *   result objects as argument
   */
  persistence.DbQueryCollection.prototype.list = function (tx, callback) {
    var args = argspec.getArgs(arguments, [
        { name: 'tx', optional: true, check: persistence.isTransaction, defaultValue: null },
        { name: 'callback', optional: false, check: argspec.isCallback() }
      ]);
    tx = args.tx;
    callback = args.callback;

    var that = this;
    var session = this._session;
    if(!tx) { // no transaction supplied
      session.transaction(function(tx) {
          that.list(tx, callback);
        });
      return;
    }
    var entityName = this._entityName;
    var meta = persistence.getMeta(entityName);
    var tm = persistence.typeMapper;

    // handles mixin case -- this logic is generic and could be in persistence.
    if (meta.isMixin) {
      var result = [];
      persistence.asyncForEach(meta.mixedIns, function(realMeta, next) {
        var query = that.clone();
        query._entityName = realMeta.name;
        query.list(tx, function(array) {
          result = result.concat(array);
          next();
        });
      }, function() {
        var query = new persistence.LocalQueryCollection(result);
        query._orderColumns = that._orderColumns;
        query._reverse = that._reverse;
        // TODO: handle skip and limit -- do we really want to do it?
        query.list(null, callback);
      });
      return;
    }

    function selectAll (meta, tableAlias, prefix) {
      var selectFields = [ tm.inIdVar("`" + tableAlias + "`.id") + " AS " + prefix + "id" ];
      for ( var p in meta.fields) {
        if (meta.fields.hasOwnProperty(p)) {
          selectFields.push(tm.inVar("`" + tableAlias + "`.`" + p + "`", meta.fields[p]) + " AS `"
            + prefix + p + "`");
        }
      }
      for ( var p in meta.hasOne) {
        if (meta.hasOne.hasOwnProperty(p)) {
          selectFields.push(tm.inIdVar("`" + tableAlias + "`.`" + p + "`") + " AS `"
            + prefix + p + "`");
        }
      }
      return selectFields;
    }
    var args = [];
    var mainPrefix = entityName + "_";

    var mainAlias = 'root';
    var selectFields = selectAll(meta, mainAlias, mainPrefix);

    var joinSql = this.joinSql(mainAlias);

    for ( var i = 0; i < this._prefetchFields.length; i++) {
      var prefetchField = this._prefetchFields[i];
      var thisMeta = meta.hasOne[prefetchField].type.meta;
      if (thisMeta.isMixin)
        throw new Error("cannot prefetch a mixin");
      var tableAlias = thisMeta.name + '_' + prefetchField + "_tbl";
      selectFields = selectFields.concat(selectAll(thisMeta, tableAlias,
          prefetchField + "_"));
      joinSql += "LEFT JOIN `" + thisMeta.name + "` AS `" + tableAlias
      + "` ON `" + tableAlias + "`.`id` = `" + mainAlias + '`.`' + prefetchField + "` ";

    }

    var sql = "SELECT " + selectFields.join(", ")
            + this.fromSql(mainAlias)
            + joinSql
            + this.whereSql(mainAlias, args);

    if(this._orderColumns.length > 0) {
      sql += " ORDER BY "
      + this._orderColumns.map(
        function (c) {
          return (c[2] ? "`" : "LOWER(`") + mainPrefix + c[0] + (c[2] ? "` " : "`) ")
          + (c[1] ? "ASC" : "DESC");
        }).join(", ");
    }
    if(this._limit >= 0) {
      sql += " LIMIT " + this._limit;
    }
    if(this._skip > 0) {
      sql += " OFFSET " + this._skip;
    }
    session.flush(tx, function () {
        tx.executeSql(sql, args, function (rows) {
            var results = [];
            if(that._reverse) {
              rows.reverse();
            }
            for ( var i = 0; i < rows.length; i++) {
              var r = rows[i];
              var e = rowToEntity(session, entityName, r, mainPrefix);
              for ( var j = 0; j < that._prefetchFields.length; j++) {
                var prefetchField = that._prefetchFields[j];
                var thisMeta = meta.hasOne[prefetchField].type.meta;
                e._data_obj[prefetchField] = rowToEntity(session, thisMeta.name, r, prefetchField + '_');
                session.add(e._data_obj[prefetchField]);
              }
              results.push(e);
              session.add(e);
            }
            callback(results);
            that.triggerEvent('list', that, results);
          });
      });
  };

  /**
   * Asynchronous call to remove all the items in the collection.
   * Note: does not only remove the items from the collection, but
   * the items themselves.
   * @param tx transaction to use
   * @param callback function to be called when clearing has completed
   */
  persistence.DbQueryCollection.prototype.destroyAll = function (tx, callback) {
    var args = argspec.getArgs(arguments, [
        { name: 'tx', optional: true, check: persistence.isTransaction, defaultValue: null },
        { name: 'callback', optional: true, check: argspec.isCallback(), defaultValue: function(){} }
      ]);
    tx = args.tx;
    callback = args.callback;

    var that = this;
    var session = this._session;
    if(!tx) { // no transaction supplied
      session.transaction(function(tx) {
          that.destroyAll(tx, callback);
        });
      return;
    }
    var entityName = this._entityName;
    var meta = persistence.getMeta(entityName);
    var tm = persistence.typeMapper;

    // handles mixin case -- this logic is generic and could be in persistence.
    if (meta.isMixin) {
      persistence.asyncForEach(meta.mixedIns, function(realMeta, next) {
        var query = that.clone();
        query._entityName = realMeta.name;
        query.destroyAll(tx, callback);
      }, callback);
      return;
    }

    var joinSql = this.joinSql("root");
    var args = [];

    var selectSql = "SELECT id" + this.fromSql("root") + " " + joinSql + this.whereSql("root", args);
    var deleteSql = "DELETE" + this.fromSql() + " WHERE id IN (" + selectSql + ")";
    var args2 = args.slice(0);

    session.flush(tx, function () {
        tx.executeSql(selectSql, args, function(results) {
            for(var i = 0; i < results.length; i++) {
              delete session.trackedObjects[results[i].id];
              persistence.triggerGlobalEvent('remove', results[i]);
            }
            tx.executeSql(deleteSql, args2, callback, callback);
          }, callback);
      });
  };

  /**
   * Asynchronous call to count the number of items in the collection.
   * @param tx transaction to use
   * @param callback function to be called when clearing has completed
   */
  persistence.DbQueryCollection.prototype.count = function (tx, callback) {
    var args = argspec.getArgs(arguments, [
        { name: 'tx', optional: true, check: persistence.isTransaction, defaultValue: null },
        { name: 'callback', optional: false, check: argspec.isCallback() }
      ]);
    tx = args.tx;
    callback = args.callback;

    var that = this;
    var session = this._session;
    if(tx && !tx.executeSql) { // provided callback as first argument
      callback = tx;
      tx = null;
    }
    if(!tx) { // no transaction supplied
      session.transaction(function(tx) {
          that.count(tx, callback);
        });
      return;
    }
    var entityName = this._entityName;
    var meta = persistence.getMeta(entityName);
    var tm = persistence.typeMapper;

    // handles mixin case -- this logic is generic and could be in persistence.
    if (meta.isMixin) {
      var result = 0;
      persistence.asyncForEach(meta.mixedIns, function(realMeta, next) {
        var query = that.clone();
        query._entityName = realMeta.name;
        query.count(tx, function(count) {
          result += count;
          next();
        });
      }, function() {
        callback(result);
      });
      return;
    }

    var joinSql = this.joinSql("root");
    var args = [];

    var skipLimitSql = '';
    if(this._limit >= 0) {
      skipLimitSql += " LIMIT " + this._limit;
    }
    if(this._skip > 0) {
      skipLimitSql += " OFFSET " + this._skip;
    }

    if (skipLimitSql === '') {
      var sql = "SELECT COUNT(*) AS cnt" + this.fromSql("root") + " " + joinSql + " " + this.whereSql("root", args);
    } else {
      var sql = "SELECT COUNT(*) AS cnt FROM (SELECT id " + this.fromSql("root") + " " + joinSql + " " + this.whereSql("root", args) + skipLimitSql + ")";
    }

    session.flush(tx, function () {
        tx.executeSql(sql, args, function(results) {
            callback(parseInt(results[0].cnt, 10));
          });
      });
  };

  /**
   * Function called when session is flushed, returns list of SQL queries to execute
   * (as [query, arg] tuples)
   */
  persistence.DbQueryCollection.prototype.persistQueries = function() { return []; };

  var originalClone = persistence.ManyToManyDbQueryCollection.prototype.clone;

  persistence.ManyToManyDbQueryCollection.prototype.clone = function () {
    var c = originalClone.call(this);
    c._manyToManyFetch = this._manyToManyFetch;
    c._meta = this._meta;
    c._rel = this._rel;
    c._inverseMeta = this._inverseMeta;
    c._inv = this._inv;
    return c;
  };

  persistence.ManyToManyDbQueryCollection.prototype.init = function(session, entityName, constructor) {
    persistence.QueryCollection.prototype.init.call(this, session, entityName, constructor);
    this._manyToManyFetch = null;
  };

  var originalInitManyToMany = persistence.ManyToManyDbQueryCollection.prototype.initManyToMany;
  persistence.ManyToManyDbQueryCollection.prototype.initManyToMany = function(coll, obj) {
    originalInitManyToMany.call(this, coll, obj);
    this._meta = persistence.getMeta(this._entityName);
    this._rel = this._meta.hasMany[coll];
    this._inverseMeta = this._rel.type.meta;
    this._inv = this._inverseMeta.hasMany[this._rel.inverseProperty];
    var direct = this._rel.mixin ? this._rel.mixin.meta.name : this._meta.name;
    var inverse = this._inv.mixin ? this._inv.mixin.meta.name : this._inverseMeta.name;
    this._manyToManyFetch = {
      table: this._rel.tableName,
      prop: direct + '_' + coll,
      inverseProp: inverse + '_' + this._rel.inverseProperty,
      obj: obj
    };
  };

  persistence.ManyToManyDbQueryCollection.prototype.toUniqueString = function() {
    var s = persistence.QueryCollection.prototype.toUniqueString.call(this);
    if(this._manyToManyFetch) {
      s += '|ManyToManyFetch:';
      s += this._manyToManyFetch.prop + ':' + (this._obj ? this._obj.id : 'null');
    }
    return s;
  };

  persistence.ManyToManyDbQueryCollection.prototype.joinSql = function (mainAlias) {
    var joinSql = persistence.DbQueryCollection.prototype.joinSql.call(this, mainAlias);
    var mtm = this._manyToManyFetch;
    var mtmJoin = "`" + mtm.table + "` AS mtm ON mtm.`" + mtm.prop + "` = `" + mainAlias + "`.`id`";
    return " LEFT JOIN " + mtmJoin + " " + joinSql;
  }

  persistence.ManyToManyDbQueryCollection.prototype.whereSql = function (mainAlias, args) {
    var tm = persistence.typeMapper;
    var mtm = this._manyToManyFetch;
    var whereSql = persistence.DbQueryCollection.prototype.whereSql.call(this, mainAlias, args);
    whereSql += " AND mtm.`" + mtm.inverseProp + "`";
    if (mtm.obj === null) {
      whereSql += " IS NULL"
    } else {
      whereSql += " = " + tm.outId(mtm.obj.id);
    }
    return whereSql;
  }

  persistence.ManyToManyDbQueryCollection.prototype.persistQueries = function() {
    var queries = [];

    var tm = persistence.typeMapper;
    var mtm = this._manyToManyFetch;

    // Added
    for(var i = 0; i < this._localAdded.length; i++) {
      var columns = [mtm.inverseProp, mtm.prop];
      var vars = [tm.outIdVar("?"), tm.outIdVar("?")];
      var args = [tm.entityIdToDbId(this._obj.id), tm.entityIdToDbId(this._localAdded[i].id)];
      if (this._rel.mixin) {
        columns.push(mtm.inverseProp + "_class");
        vars.push("?");
        args.push(this._meta.name);
      }
      if (this._inv.mixin) {
        columns.push(mtm.prop + "_class");
        vars.push("?");
        args.push(this._inverseMeta.name);
      }
      queries.push(["INSERT OR IGNORE INTO " + mtm.table +
            " (`" + columns.join("`, `") + "`) VALUES (" + vars.join(",") + ")", args]);
    }
    this._localAdded = [];
    // Removed
    for(var i = 0; i < this._localRemoved.length; i++) {
    queries.push(["DELETE FROM  " + mtm.table +
          " WHERE `" + mtm.inverseProp + "` = " + tm.outIdVar("?") + " AND `" +
          mtm.prop +
          "` = " + tm.outIdVar("?"), [tm.entityIdToDbId(this._obj.id), tm.entityIdToDbId(this._localRemoved[i].id)]]);
    }
    this._localRemoved = [];
    return queries;
  };
};

window = window || {};
window.persistence = window.persistence || {};
window.persistence.store = window.persistence.store || {};
window.persistence.store.sql = {
  defaultTypeMapper: defaultTypeMapper,
  config: config
};
