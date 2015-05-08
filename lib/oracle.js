/*!
 * Oracle connector for LoopBack
 */
var oracle = require('oracledb');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var util = require('util');
var async = require('async');
var debug = require('debug')('loopback:connector:oracle');
var debugConnection = require('debug')('loopback:connector:oracle:connection');

/*!
 * @module loopback-connector-oracle
 *
 * Initialize the Oracle connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!oracle) {
    return;
  }

  var s = dataSource.settings || {};
  var oracle_settings = {
    connectString: s.connectString || s.url || s.tns,
    user: s.username || s.user,
    password: s.password,
    debug: s.debug || debug.enabled,
    poolMin: s.poolMin || s.minConn || 1,
    poolMax: s.poolMax || s.maxConn || 10,
    poolIncrement: s.poolIncrement || s.incrConn || 1,
    poolTimeout: s.poolTimeout || s.timeout || 60,
    isAutoCommit: s.isAutoCommit || s.autoCommit,
    outFormat: oracle.OBJECT,
    maxRows: s.maxRows || 100,
    stmtCacheSize: s.stmtCacheSize || 30
  };

  if (s.isAutoCommit === undefined) {
    s.isAutoCommit = true; // Default to true
  }

  if (!s.connectString) {
    var hostname = s.host || s.hostname || 'localhost';
    var port = s.port || 1521;
    var database = s.database || 'XE';
    oracle_settings.connectString = '//' + hostname + ':' + port +
      '/' + database;
  }

  /*
  for (var p in s) {
    if (!(p in oracle_settings)) {
      oracle_settings[p] = s[p];
    }
  }
  */

  dataSource.connector = new Oracle(oracle, oracle_settings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

exports.Oracle = Oracle;

/**
 * Oracle connector constructor
 *
 *
 * @param {object} driver Oracle node.js binding
 * @options {Object} settings Options specifying data source settings; see below.
 * @prop {String} hostname The host name or ip address of the Oracle DB server
 * @prop {Number} port The port number of the Oracle DB Server
 * @prop {String} user The user name
 * @prop {String} password The password
 * @prop {String} database The database name (TNS listener name)
 * @prop {Boolean|Number} debug If true, print debug messages. If Number, ?
 * @class
 */
function Oracle(oracle, settings) {
  this.constructor.super_.call(this, 'oracle', settings);
  this.driver = oracle;
  this.pool = null;
  this.parallelLimit = settings.maxConn;
  if (settings.debug || debug.enabled) {
    debug('Settings: %j', settings);
  }
}

// Inherit from loopback-datasource-juggler BaseSQL
require('util').inherits(Oracle, SqlConnector);

Oracle.prototype.debug = function () {
  if (this.settings.debug || debug.enabled) {
    debug.apply(null, arguments);
  }
};

/**
 * Connect to Oracle
 * @param {Function} [callback] The callback after the connection is established
 */
Oracle.prototype.connect = function (callback) {
  var self = this;
  if (this.pool) {
    if (callback) {
      process.nextTick(function () {
        callback && callback(null, self.pool);
      });
    }
    return;
  }
  if (this.settings.debug) {
    this.debug('Connecting to ' + this.settings.hostname);
  }
  this.driver.createPool(this.settings, function (err, pool) {
    if (!err) {
      self.pool = pool;
      if (self.settings.debug) {
        self.debug('Connected to ' + self.settings.hostname);
        self.debug('Connection pool ', pool);
      }
      callback && callback(err, pool);
    } else {
      console.error(err);
      throw err;
    }
  });
};

/**
 * Execute the SQL statement.
 *
 * @param {String} sql The SQL statement.
 * @param {String[]} params The parameter values for the SQL statement.
 * @param {Function} [callback] The callback after the SQL statement is executed.
 */
Oracle.prototype.executeSQL = function (sql, params, options, callback) {
  var self = this;

  if (self.settings.debug) {
    if (params && params.length > 0) {
      self.debug('SQL: %s \nParameters: %j', sql, params);
    } else {
      self.debug('SQL: %s', sql);
    }
  }
  self.pool.getConnection(function (err, connection) {
    if (err) {
      callback && callback(err);
      return;
    }
    debug('open connection: %d -> %d', open, ++open);
    if (self.settings.debug) {
      debugConnection('Connection acquired: ', self.pool);
    }
    connection.clientId = self.settings.clientId || 'LoopBack';
    connection.module = self.settings.module || 'loopback-connector-oracle';
    connection.action = self.settings.action || '';

    connection.execute(sql, params,
      {outFormat: oracle.OBJECT, isAutoCommit: true},
      function (err, data) {
      if (err && self.settings.debug) {
        self.debug(err);
      }
      if (self.settings.debug && data) {
        self.debug("Result: %j", data);
      }
      if (log) {
        log(sql, time);
      }
      connection.release(function(err) {
        if (err) {
          self.debug(err);
        }
      });
      debug('close connection: %d -> %d', open, --open);
      if (self.settings.debug) {
        self.debug('Connection released: ', self.pool);
      }
      if (!err && data) {
        if (data.outBinds) {
          data = {returnParam: data.outBinds};
        } else if (data.rows) {
          data = data.rows;
        } else {
          data = data.rowsAffected;
        }
      }
      callback(err ? err : null, data ? data : null);
    });
  });
  // self.pool.execute(sql, params, callback);
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
Oracle.prototype.getPlaceholderForValue = function(key) {
  return ':' + key;
};

Oracle.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.updateCount;
};

Oracle.prototype.getInsertedId = function(model, info) {
  return info && info.returnParam;
};

Oracle.prototype.buildInsertDefaultValues = function(model, data, options) {
  // Oracle doesn't like empty column/value list
  var idCol = this.idColumnEscaped(model);
  return '(' + idCol + ') VALUES(DEFAULT)';
};

Oracle.prototype.buildInsertReturning = function(model, data, options) {
  var modelDef = this.getModelDefinition(model);
  var type = modelDef.properties[this.idName(model)].type;
  var outParam = null;
  if (type === Number) {
    outParam = {type: oracle.NUMBER, dir: oracle.BIND_OUT};
  } else if (type === Date) {
    outParam = {type: oracle.DATE, dir: oracle.BIND_OUT};
  } else {
    outParam = {type: oracle.STRING, dir: oracle.BIND_OUT};
  }
  var params = [outParam];
  var returningStmt = new ParameterizedSQL('RETURNING ' +
    this.idColumnEscaped(model) + ' into ?', params);
  return returningStmt;
};

/**
 * Create the data model in Oracle
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Function} [callback] The callback function
 */
Oracle.prototype.create = function(model, data, options, callback) {
  var self = this;
  var stmt = this.buildInsert(model, data, options);
  this.execute(stmt.sql, stmt.params, function(err, info) {
    if (err) {
      if (err.toString().indexOf('ORA-00001: unique constraint') >= 0) {
        // Transform the error so that duplicate can be checked using regex
        err = new Error(err.toString() + '. Duplicate id detected.');
      }
      callback(err);
    } else {
      var insertedId = self.getInsertedId(model, info);
      callback(err, insertedId);
    }
  });
};

function dateToOracle(val, dateOnly) {
  function fz(v) {
    return v < 10 ? '0' + v : v;
  }

  function ms(v) {
    if (v < 10) {
      return '00' + v;
    } else if (v < 100) {
      return '0' + v;
    } else {
      return '' + v;
    }
  }

  var dateStr = [
    val.getUTCFullYear(),
    fz(val.getUTCMonth() + 1),
    fz(val.getUTCDate())
  ].join('-') + ' ' + [
    fz(val.getUTCHours()),
    fz(val.getUTCMinutes()),
    fz(val.getUTCSeconds())
  ].join(':');

  if (!dateOnly) {
    dateStr += '.' + ms(val.getMilliseconds());
  }

  if (dateOnly) {
    return new ParameterizedSQL(
      "to_date(?,'yyyy-mm-dd hh24:mi:ss')", [dateStr]);
  } else {
    return new ParameterizedSQL(
      "to_timestamp(?,'yyyy-mm-dd hh24:mi:ss.ff3')", [dateStr]);
  }

}

Oracle.prototype.toColumnValue = function(prop, val) {
  if (val == null) {
    // PostgreSQL complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('DEFAULT');
    }
    else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    return dateToOracle(val, prop.type === Date);
  }

  // Oracle support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return 'Y';
    } else {
      return 'N';
    }
  }

  return this.serializeObject(val);
};

Oracle.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  var type = prop && prop.type;
  if (type === Boolean) {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (val === 'Y' || val === 'y' || val === 'T' ||
      val === 't' || val === '1');
    }
  }
  return val;
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
Oracle.prototype.dbName = function (name) {
  if (!name) {
    return name;
  }
  return name.toUpperCase();
};

/*!
 * Escape the name for Oracle DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
Oracle.prototype.escapeName = function (name) {
  if (!name) {
    return name;
  }
  return '"' + name.replace(/\./g, '"."') + '"';
};


Oracle.prototype.tableEscaped = function (model) {
  var schemaName = this.schema(model);
  if (schemaName && schemaName !== this.settings.user) {
    return this.escapeName(schemaName) + '.' +
      this.escapeName(this.table(model));
  } else {
    return this.escapeName(this.table(model));
  }
};

Oracle.prototype.buildExpression =
  function(columnName, operator, columnValue, propertyValue) {
    if (propertyValue instanceof RegExp) {
      columnValue = "'" + propertyValue.source + "'";
      if (propertyValue.ignoreCase) {
        return new ParameterizedSQL(columnName + ' ~* ?', [columnValue]);
      } else {
        return new ParameterizedSQL(columnName + ' ~ ?', [columnValue]);
      }
    }
    switch(operator) {
      case 'like':
        return new ParameterizedSQL({
          sql: columnName + " LIKE ? ESCAPE '\\'",
          params: [columnValue]
        });
      case 'nlike':
        return new ParameterizedSQL({
          sql: columnName + " NOT LIKE ? ESCAPE '\\'",
          params: [columnValue]
        });
      default:
        // Invoke the base implementation of `buildExpression`
        var exp = this.invokeSuper('buildExpression',
          columnName, operator, columnValue, propertyValue);
        return exp;
    }
  };

function buildLimit(limit, offset) {
  if (isNaN(offset)) {
    offset = 0;
  }
  var sql = 'OFFSET ' + offset + ' ROWS';
  if (limit >= 0) {
    sql += ' FETCH NEXT ' + limit + ' ROWS ONLY';
  }
  return sql;
}

Oracle.prototype.applyPagination =
  function(model, stmt, filter) {
    var offset = filter.offset || filter.skip || 0;
    if (this.settings.supportsOffsetFetch) {
      // Oracle 12.c or later
      var limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
      return stmt.merge(limitClause);
    } else {
      var paginatedSQL = 'SELECT * FROM (' + stmt.sql + ' ' +
        ')' + ' ' + ' WHERE R > ' + offset;

      if (filter.limit !== -1) {
        paginatedSQL += ' AND R <= ' + (offset + filter.limit);
      }

      stmt.sql = paginatedSQL + ' ';
      return stmt;
    }
  };

Oracle.prototype.buildColumnNames = function(model, filter) {
  var columnNames = this.invokeSuper('buildColumnNames', model, filter);
  if (filter.limit || filter.offset || filter.skip) {
    var orderBy = this.buildOrderBy(model, filter.order);
    columnNames += ',ROW_NUMBER() OVER' + ' (' + orderBy + ') R';
  }
  return columnNames;
};

Oracle.prototype.buildSelect = function(model, filter, options) {
  if (!filter.order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }

  var selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter) +
    ' FROM ' + this.tableEscaped(model)
  );

  if (filter) {

    if (filter.where) {
      var whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter);
    } else {
      if (filter.order) {
        selectStmt.merge(this.buildOrderBy(model, filter.order));
      }
    }

  }
  return this.parameterize(selectStmt);
};

/**
 * Disconnect from Oracle
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.disconnect = function disconnect(cb) {
  var err = null;
  if (this.pool) {
    if (this.settings.debug) {
      this.debug('Disconnecting from ' + this.settings.hostname);
    }
    var pool = this.pool;
    this.pool = null;
    try {
      pool.close();  // This is sync
    } catch (e) {
      err = e;
      this.debug('Fail to disconnect from %s: %j', this.settings.hostname, err);
    }
  }

  if (cb) {
    process.nextTick(function () {
      cb(err);
    });
  }
};

Oracle.prototype.ping = function (cb) {
  this.execute('select count(*) as result from user_tables', [], cb);
};

<<<<<<< HEAD
  if (options.all && !owner) {
    sqlTables = paginateSQL('SELECT \'table\' AS "type", table_name AS "name", owner AS "owner"'
      + ' FROM all_tables', 'owner, table_name', options);
  } else if (owner) {
    sqlTables = paginateSQL('SELECT \'table\' AS "type", table_name AS "name", owner AS "owner"'
      + ' FROM all_tables WHERE owner=\'' + owner + '\'', 'owner, table_name', options);
  } else {
    sqlTables = paginateSQL('SELECT \'table\' AS "type", table_name AS "name",'
      + ' SYS_CONTEXT(\'USERENV\', \'SESSION_USER\') AS "owner" FROM user_tables',
      'table_name', options);
  }
  return sqlTables;
}

/*!
 * Build sql for listing views
 * @param {Object} options {all: for all owners, owner: for a given owner}
 * @returns {String} The sql statement
 */
function queryViews(options) {
  var sqlViews = null;
  if (options.views) {

    var owner = options.owner || options.schema;

    if (options.all && !owner) {
      sqlViews = paginateSQL('SELECT \'view\' AS "type", view_name AS "name",'
        + ' owner AS "owner" FROM all_views',
        'owner, view_name', options);
    } else if (owner) {
      sqlViews = paginateSQL('SELECT \'view\' AS "type", view_name AS "name",'
        + ' owner AS "owner" FROM all_views WHERE owner=\'' + owner + '\'',
        'owner, view_name', options);
    } else {
      sqlViews = paginateSQL('SELECT \'view\' AS "type", view_name AS "name",'
        + ' SYS_CONTEXT(\'USERENV\', \'SESSION_USER\') AS "owner" FROM user_views',
        'view_name', options);
    }
  }
  return sqlViews;
}

/**
 * Discover model definitions.
 * Example of callback function return value:
 * ```js
 * {type: 'table', name: 'INVENTORY', owner: 'STRONGLOOP' }
 * {type: 'table', name: 'LOCATION', owner: 'STRONGLOOP' }
 * {type: 'view', name: 'INVENTORY_VIEW', owner: 'STRONGLOOP' }
 *```
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @param {Function} [cb] The callback function.
 * @private
 */
Oracle.prototype.discoverModelDefinitions = function (options, cb) {
  if (!cb && typeof options === 'function') {
    cb = options;
    options = {};
  }
  options = options || {};

  var self = this;
  var calls = [function (callback) {
    self.query(queryTables(options), callback);
  }];

  if (options.views) {
    calls.push(function (callback) {
      self.query(queryViews(options), callback);
    });
  }
  async.parallelLimit(calls, this.parallelLimit, function (err, data) {
    if (err) {
      cb(err, data);
    } else {
      var merged = [];
      merged = merged.concat(data.shift());
      if (data.length) {
        merged = merged.concat(data.shift());
      }
      cb(err, merged);
    }
  });
};

/**
 * Discover the model definitions synchronously.
 * 
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @private
 */
Oracle.prototype.discoverModelDefinitionsSync = function (options) {
  options = options || {};
  var sqlTables = queryTables(options);
  var tables = this.querySync(sqlTables);
  var sqlViews = queryViews(options);
  if (sqlViews) {
    var views = this.querySync(sqlViews);
    tables = tables.concat(views);
  }
  return tables;
};

/*!
 * Normalize the arguments
 * @param {String} table The table name
 * @param {Object} [options] The options object
 * @param {Function} [cb] The callback function
 */
function getArgs(table, options, cb) {
  if ('string' !== typeof table || !table) {
    throw new Error('table is a required string argument: ' + table);
  }
  options = options || {};
  if (!cb && 'function' === typeof options) {
    cb = options;
    options = {};
  }
  if (typeof options !== 'object') {
    throw new Error('options must be an object: ' + options);
  }
  return {
    owner: options.owner || options.schema,
    table: table,
    options: options,
    cb: cb
  };
}

/*!
 * Build the sql statement to query columns for a given table
 * @param {String} owner The DB owner/schema name
 * @param {String} table The table name
 * @returns {String} The sql statement
 */
function queryColumns(owner, table) {
  var sql = null;
  if (owner) {
    sql = paginateSQL('SELECT owner AS "owner", table_name AS "tableName",'
      + ' column_name AS "columnName", data_type AS "dataType",'
      + ' data_length AS "dataLength", data_precision AS "dataPrecision",'
      + ' data_scale AS "dataScale", nullable AS "nullable"'
      + ' FROM all_tab_columns'
      + ' WHERE owner=\'' + owner + '\''
      + (table ? ' AND table_name=\'' + table + '\'' : ''),
      'table_name, column_id', {});
  } else {
    sql = paginateSQL('SELECT SYS_CONTEXT(\'USERENV\', \'SESSION_USER\') AS "owner",'
      + ' table_name AS "tableName", column_name AS "columnName", data_type AS "dataType",'
      + ' data_length AS "dataLength", data_precision AS "dataPrecision",'
      + ' data_scale AS "dataScale", nullable AS "nullable"'
      + ' FROM user_tab_columns'
      + (table ? ' WHERE table_name=\'' + table + '\'' : ''),
      'table_name, column_id', {});
  }
  return sql;
}

/**
 * Discover model properties from a table.  Returns an array of columns for the specified table.
 *
 * @param {String} table The table name
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @param {Function} [cb] The callback function
 * 
 * ```js
 * { owner: 'STRONGLOOP',
 *   tableName: 'PRODUCT',
 *   columnName: 'ID',
 *   dataType: 'VARCHAR2',
 *   dataLength: 20,
 *   nullable: 'N',
 *   type: 'String'
 * }
 * { owner: 'STRONGLOOP',
 *   tableName: 'PRODUCT',
 *   columnName: 'NAME',
 *   dataType: 'VARCHAR2',
 *   dataLength: 64,
 *   nullable: 'Y',
 *   type: 'String'
 * }
 *```
 * @private
 */
Oracle.prototype.discoverModelProperties = function (table, options, cb) {
  var args = getArgs(table, options, cb);
  var owner = args.owner;
  table = args.table;
  options = args.options;
  cb = args.cb;

  var sql = queryColumns(owner, table);
  var callback = function (err, results) {
    if (err) {
      cb(err, results);
    } else {
      results.map(function (r) {
        r.type = oracleDataTypeToJSONType(r.dataType, r.dataLength);
      });
      cb(err, results);
    }
  };
  this.query(sql, callback);
}

/**
 * Discover model properties from a table synchronously.  See example return value for discoverModelProperties().
 * 
 * @param {String} table The table name
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @private
 * 
 */
Oracle.prototype.discoverModelPropertiesSync = function (table, options) {
  var args = getArgs(table, options);
  var owner = args.owner;
  table = args.table;
  options = args.options;

  var sql = queryColumns(owner, table);
  var results = this.querySync(sql);
  results.map(function (r) {
    r.type = oracleDataTypeToJSONType(r.dataType, r.dataLength);
  });
  return results;
}

/*!
 * Build the sql statement for querying primary keys of a given table
 * @param owner
 * @param table
 * @returns {String}
 */
// http://docs.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#
// getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
function queryForPrimaryKeys(owner, table) {
  var sql = 'SELECT uc.owner AS "owner", '
    + 'uc.table_name AS "tableName", col.column_name AS "columnName",'
    + ' col.position AS "keySeq", uc.constraint_name AS "pkName" FROM'
    + (owner ? ' ALL_CONSTRAINTS uc, ALL_CONS_COLUMNS col' : ' USER_CONSTRAINTS uc, USER_CONS_COLUMNS col')
    + ' WHERE uc.constraint_type=\'P\' AND uc.constraint_name=col.constraint_name';

  if (owner) {
    sql += ' AND uc.owner=\'' + owner + '\'';
  }
  if (table) {
    sql += ' AND uc.table_name=\'' + table + '\'';
  }
  sql += ' ORDER BY uc.owner, col.constraint_name, uc.table_name, col.position';
  return sql;
}

/**
 * Discover primary keys for specified table. Returns an array of primary keys for the specified table.
 * Example return value:
 * ```js
 *         { owner: 'STRONGLOOP',
 *           tableName: 'INVENTORY',
 *           columnName: 'PRODUCT_ID',
 *           keySeq: 1,
 *           pkName: 'ID_PK' }
 *         { owner: 'STRONGLOOP',
 *           tableName: 'INVENTORY',
 *           columnName: 'LOCATION_ID',
 *           keySeq: 2,
 *          pkName: 'ID_PK' }
 *``` 
 * 
 * @param {String} table The table name
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @param {Function} [cb] The callback function
 * @private
 */
Oracle.prototype.discoverPrimaryKeys = function (table, options, cb) {
  var args = getArgs(table, options, cb);
  var owner = args.owner;
  table = args.table;
  options = args.options;
  cb = args.cb;

  var sql = queryForPrimaryKeys(owner, table);
  this.query(sql, cb);
}

/**
 * Discover primary keys synchronously for specified table.  See example return value for discoverPrimaryKeys().
 * 
 * @param {String} table
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @private
 */
Oracle.prototype.discoverPrimaryKeysSync = function (table, options) {
  var args = getArgs(table, options);
  var owner = args.owner;
  table = args.table;
  options = args.options;

  var sql = queryForPrimaryKeys(owner, table);
  return this.querySync(sql);
}

/*!
 * Build the sql statement for querying foreign keys of a given table
 * @param {String} owner The DB owner/schema name
 * @param {String} table The table name
 * @returns {String} The SQL statement to find foreign keys
 */
function queryForeignKeys(owner, table) {
  var sql =
    'SELECT a.owner AS "fkOwner", a.constraint_name AS "fkName", a.table_name AS "fkTableName",'
      + ' a.column_name AS "fkColumnName", a.position AS "keySeq",'
      + ' jcol.owner AS "pkOwner", jcol.constraint_name AS "pkName",'
      + ' jcol.table_name AS "pkTableName", jcol.column_name AS "pkColumnName"'
      + ' FROM'
      + ' (SELECT'
      + ' uc.owner, uc.table_name, uc.constraint_name, uc.r_constraint_name, col.column_name, col.position'
      + ' FROM'
      + (owner ? ' ALL_CONSTRAINTS uc, ALL_CONS_COLUMNS col' : ' USER_CONSTRAINTS uc, USER_CONS_COLUMNS col')
      + ' WHERE'
      + ' uc.constraint_type=\'R\' and uc.constraint_name=col.constraint_name';
  if (owner) {
    sql += ' AND uc.owner=\'' + owner + '\'';
  }
  if (table) {
    sql += ' AND uc.table_name=\'' + table + '\'';
  }
  sql += ' ) a'
    + ' INNER JOIN'
    + ' USER_CONS_COLUMNS jcol'
    + ' ON'
    + ' a.r_constraint_name=jcol.constraint_name';
  return sql;
}

/**
 * Discover foreign keys for specified table.  Example return value:
 * ```js
 * { fkOwner: 'STRONGLOOP',
 *   fkName: 'PRODUCT_FK',
 *   fkTableName: 'INVENTORY',
 *   fkColumnName: 'PRODUCT_ID',
 *   keySeq: 1,
 *   pkOwner: 'STRONGLOOP',
 *   pkName: 'PRODUCT_PK',
 *   pkTableName: 'PRODUCT',
 *   pkColumnName: 'ID' }
 * ```
 * 
 * @param {String} table The table name
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @param {Function} [cb] The callback function
 * @private
 */
Oracle.prototype.discoverForeignKeys = function (table, options, cb) {
  var args = getArgs(table, options, cb);
  var owner = args.owner;
  table = args.table;
  options = args.options;
  cb = args.cb;

  var sql = queryForeignKeys(owner, table);
  this.query(sql, cb);
};

/**
 * Discover foreign keys synchronously for a given table.  See example return value for discoverForeignKeys().
 * 
 * @param {String} table The table name
 * @options {Object} options Options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @prop {Number} limit Maximimum number of results to return.  NOTE: This was not in .md doc, but is included in tests.
 * @private
 */
Oracle.prototype.discoverForeignKeysSync = function (table, options) {
  var args = getArgs(table, options);
  var owner = args.owner;
  table = args.table;
  options = args.options;

  var sql = queryForeignKeys(owner, table);
  return this.querySync(sql);
};

/*!
 * Retrieves a description of the foreign key columns that reference the given
 * table's primary key columns (the foreign keys exported by a table).
 * They are ordered by fkTableOwner, fkTableName, and keySeq.
 * @param {String} owner The DB owner/schema name
 * @param {String} table The table name
 * @returns {String} The SQL statement
 */
function queryExportedForeignKeys(owner, table) {
  var sql = 'SELECT a.constraint_name AS "fkName", a.owner AS "fkOwner", a.table_name AS "fkTableName",'
    + ' a.column_name AS "fkColumnName", a.position AS "keySeq",'
    + ' jcol.constraint_name AS "pkName", jcol.owner AS "pkOwner",'
    + ' jcol.table_name AS "pkTableName", jcol.column_name AS "pkColumnName"'
    + ' FROM'
    + ' (SELECT'
    + ' uc1.table_name, uc1.constraint_name, uc1.r_constraint_name, col.column_name, col.position, col.owner'
    + ' FROM'
    + (owner ? ' ALL_CONSTRAINTS uc, ALL_CONSTRAINTS uc1, ALL_CONS_COLUMNS col'
    : ' USER_CONSTRAINTS uc, USER_CONSTRAINTS uc1, USER_CONS_COLUMNS col')
    + ' WHERE'
    + ' uc.constraint_type=\'P\' and uc1.r_constraint_name = uc.constraint_name and uc1.constraint_type = \'R\''
    + ' and uc1.constraint_name=col.constraint_name';
  if (owner) {
    sql += ' and col.owner=\'' + owner + '\'';
  }
  if (table) {
    sql += ' and uc.table_Name=\'' + table + '\'';
  }
  sql += ' ) a'
    + ' INNER JOIN'
    + ' USER_CONS_COLUMNS jcol'
    + ' ON'
    + ' a.r_constraint_name=jcol.constraint_name'
    + ' order by a.owner, a.table_name, a.position';

  return sql;
}

/**
 * Discover foreign keys that reference to the primary key of this table.
 * Example return value:
 * ```js
 * { fkName: 'PRODUCT_FK',
 *   fkOwner: 'STRONGLOOP',
 *   fkTableName: 'INVENTORY',
 *   fkColumnName: 'PRODUCT_ID',
 *   keySeq: 1,
 *   pkName: 'PRODUCT_PK',
 *   pkOwner: 'STRONGLOOP',
 *   pkTableName: 'PRODUCT',
 *   pkColumnName: 'ID' }
 *   ````
 * @param {String} table The table name
 * @options {Object} options The options for discovery
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @param {Function} [cb] The callback function
 * @private
 */
Oracle.prototype.discoverExportedForeignKeys = function (table, options, cb) {
  var args = getArgs(table, options, cb);
  var owner = args.owner;
  table = args.table;
  options = args.options;
  cb = args.cb;

  var sql = queryExportedForeignKeys(owner, table);
  this.query(sql, cb);
};

/**
 * Discover foreign keys synchronously for a given table; see example return value for discoverExportedForeignKeys().
 * @param {String} owner The DB owner/schema name
 * @options {Object} options The options for discovery; see below.
 * @prop {Boolean} all If true, include tables/views from all schemas/owners.
 * @prop {String} owner/schema The schema/owner name.  QUESTION:  What is the actual name of this property: 'owner' or 'schema'?  Or can it be either?
 * @prop {Boolean} views If true, include views.
 * @returns {*}
 * @private
 */
Oracle.prototype.discoverExportedForeignKeysSync = function (table, options) {
  var args = getArgs(table, options);
  var owner = args.owner;
  table = args.table;
  options = args.options;

  var sql = queryExportedForeignKeys(owner, table);
  return this.querySync(sql);
};

/**
 * Perform autoupdate for the given models
 * @param {String[]} [models] A model name or an array of model names. If not
 * present, apply to all models
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.autoupdate = function(models, cb) {
  var self = this;
  if ((!cb) && ('function' === typeof models)) {
    cb = models;
    models = undefined;
  }
  // First argument is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = models || Object.keys(this._models);

  async.eachLimit(models, this.parallelLimit, function(model, done) {
    if (!(model in self._models)) {
      return process.nextTick(function() {
        done(new Error('Model not found: ' + model));
      });
    }
    getTableStatus.call(self, model, function(err, fields) {
      if (!err && fields.length) {
        self.alterTable(model, fields, done);
      } else {
        self.createTable(model, done);
      }
    });
  }, cb);
};

/*!
 * Check if the models exist
 * @param {String[]} [models] A model name or an array of model names. If not
 * present, apply to all models
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.isActual = function(models, cb) {
  var self = this;

  if ((!cb) && ('function' === typeof models)) {
    cb = models;
    models = undefined;
  }
  // First argument is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = models || Object.keys(this._models);

  var changes = [];
  async.eachLimit(models, this.parallelLimit, function(model, done) {
    getTableStatus.call(self, model, function(err, fields) {
      changes = changes.concat(getAddModifyColumns.call(self, model, fields));
      changes = changes.concat(getDropColumns.call(self, model, fields));
      done(err);
    });
  }, function done(err) {
    if (err) {
      return cb && cb(err);
    }
    var actual = (changes.length === 0);
    cb && cb(null, actual);
  });
};

/**
 * Alter the table for the given model
 * @param {String} model The model name
 * @param {Object[]} actualFields Actual columns in the table
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.alterTable = function (model, actualFields, cb) {
  var self = this;
  var pendingChanges = getAddModifyColumns.call(self, model, actualFields);
  if (pendingChanges.length > 0) {
    applySqlChanges.call(self, model, pendingChanges, function (err, results) {
      var dropColumns = getDropColumns.call(self, model, actualFields);
      if (dropColumns.length > 0) {
        applySqlChanges.call(self, model, dropColumns, cb);
      } else {
        cb && cb(err, results);
      }
    });
  } else {
    var dropColumns = getDropColumns.call(self, model, actualFields);
    if (dropColumns.length > 0) {
      applySqlChanges.call(self, model, dropColumns, cb);
    } else {
      cb && process.nextTick(cb.bind(null, null, []));
    }
  }
};

function getAddModifyColumns(model, actualFields) {
  var sql = [];
  var self = this;
  sql = sql.concat(getColumnsToAdd.call(self, model, actualFields));
  sql = sql.concat(getPropertiesToModify.call(self, model, actualFields));
  return sql;
}

function getDropColumns(model, actualFields) {
  var sql = [];
  var self = this;
  sql = sql.concat(getColumnsToDrop.call(self, model, actualFields));
  return sql;
}

function getColumnsToAdd(model, actualFields) {
  var self = this;
  var m = self._models[model];
  var propNames = Object.keys(m.properties);
  var sql = [];
  propNames.forEach(function (propName) {
    if (self.id(model, propName)) {
      return;
    }
    var found = searchForPropertyInActual.call(self, model, self.column(model, propName), actualFields);
    if (!found && propertyHasNotBeenDeleted.call(self, model, propName)) {
      sql.push(addPropertyToActual.call(self, model, propName));
    }
  });
  if (sql.length > 0) {
    sql = ['ADD', '(' + sql.join(',') + ')'];
  }
  return sql;
}

function addPropertyToActual(model, propName) {
  var self = this;
  var sqlCommand = self.columnEscaped(model, propName)
    + ' ' + self.columnDataType(model, propName)
    + (propertyCanBeNull.call(self, model, propName) ? "" : " NOT NULL");
  return sqlCommand;
}

function searchForPropertyInActual(model, propName, actualFields) {
  var self = this;
  var found = false;
  actualFields.forEach(function (f) {
    if (f.column === self.column(model, propName)) {
      found = f;
      return;
    }
  });
  return found;
}

function getPropertiesToModify(model, actualFields) {
  var self = this;
  var sql = [];
  var m = self._models[model];
  var propNames = Object.keys(m.properties);
  var found;
  propNames.forEach(function(propName) {
    if (self.id(model, propName)) {
      return;
    }
    found = searchForPropertyInActual.call(self, model, propName, actualFields);
    if (found && propertyHasNotBeenDeleted.call(self, model, propName)) {
      var column = self.columnEscaped(model, propName);
      var clause = '';
      if (datatypeChanged(propName, found)) {
        clause = column + ' ' +
          modifyDatatypeInActual.call(self, model, propName);
      }
      if (nullabilityChanged(propName, found)) {
        if (!clause) {
          clause = column;
        }
        clause = clause + ' ' +
          modifyNullabilityInActual.call(self, model, propName);
      }
      if (clause) {
        sql.push(clause);
      }
    }
  });

  if (sql.length > 0) {
    sql = ['MODIFY', '(' + sql.join(',') + ')'];
  }
  return sql;

  function datatypeChanged(propName, oldSettings) {
    var newSettings = m.properties[propName];
    if (!newSettings) {
      return false;
    }
    return oldSettings.type.toUpperCase() !== self.columnDataType(model, propName);
  }

  function nullabilityChanged(propName, oldSettings) {
    var newSettings = m.properties[propName];
    if (!newSettings) {
      return false;
    }
    var changed = false;
    if (oldSettings.nullable === 'Y' && !isNullable(newSettings)) {
      changed = true;
    }
    if (oldSettings.nullable === 'N' && isNullable(newSettings)) {
      changed = true;
    }
    return changed;
  }
}

function modifyDatatypeInActual(model, propName) {
  var self = this;
  var sqlCommand = self.columnDataType(model, propName);
  return sqlCommand;
}

function modifyNullabilityInActual(model, propName) {
  var self = this;
  var sqlCommand = '';
  if (propertyCanBeNull.call(self, model, propName)) {
    sqlCommand = sqlCommand + "NULL";
  } else {
    sqlCommand = sqlCommand + "NOT NULL";
  }
  return sqlCommand;
}

function getColumnsToDrop(model, actualFields) {
  var self = this;
  var sql = [];
  actualFields.forEach(function (actualField) {
    if (self.idColumn(model) === actualField.column) {
      return;
    }
    if (actualFieldNotPresentInModel(actualField, model)) {
      sql.push(self.escapeName(actualField.column));
    }
  });
  if (sql.length > 0) {
    sql = ['DROP', '(' + sql.join(',') + ')'];
  }
  return sql;

  function actualFieldNotPresentInModel(actualField, model) {
    return !(self.propertyName(model, actualField.column));
  }
}

function applySqlChanges(model, pendingChanges, cb) {
  var self = this;
  if (pendingChanges.length) {
    var thisQuery = 'ALTER TABLE ' + self.tableEscaped(model);
    var ranOnce = false;
    pendingChanges.forEach(function (change) {
      if (ranOnce) {
        thisQuery = thisQuery + ' ';
      }
      thisQuery = thisQuery + ' ' + change;
      ranOnce = true;
    });
    self.query(thisQuery, cb);
  }
}

/*!
 * Build a list of columns for the given model
 * @param {String} model The model name
 * @returns {String}
 */
Oracle.prototype.propertiesSQL = function (model) {
  var self = this;
  var sql = [];
  var pks = this.idNames(model).map(function (i) {
    return self.columnEscaped(model, i);
  });
  Object.keys(this._models[model].properties).forEach(function (prop) {
    var colName = self.columnEscaped(model, prop);
    sql.push(colName + ' ' + self.propertySettingsSQL(model, prop));
  });
  if (pks.length > 0) {
    sql.push('PRIMARY KEY(' + pks.join(',') + ')');
  }
  return sql.join(',\n  ');

};

/*!
 * Build settings for the model property
 * @param {String} model The model name
 * @param {String} propName The property name
 * @returns {*|string}
 */
Oracle.prototype.propertySettingsSQL = function (model, propName) {
  var self = this;
  var result = self.columnDataType(model, propName);
  if (!propertyCanBeNull.call(self, model, propName)) result = result + ' NOT NULL';
  return result;
};

Oracle.prototype._isIdGenerated = function (model) {
  var idNames = this.idNames(model);
  if (!idNames) {
    return false;
  }
  var idName = idNames[0];
  var id = this._models[model].properties[idName];
  var idGenerated = idNames.length > 1 ? false : id && id.generated;
  return idGenerated;
};

/**
 * Drop a table for the given model
 * @param {String} model The model name
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.dropTable = function (model, cb) {
  var self = this;
  var name = self.tableEscaped(model);
  var seqName = self.escapeName(model + '_ID_SEQUENCE');

  var count = 0;
  var dropTableFun = function (callback) {
    self.query('DROP TABLE ' + name, function (err, data) {
      if (err && err.toString().indexOf('ORA-00054') >= 0) {
        count++;
        if (count <= 5) {
          self.debug('Retrying ' + count + ': ' + err);
          // Resource busy, try again
          setTimeout(dropTableFun, 200 * Math.pow(count, 2));
          return;
        }
      }
      if (err && err.toString().indexOf('ORA-00942') >= 0) {
        err = null; // Ignore it
      }
      callback(err, data);
    });
  };

  var tasks = [dropTableFun];
  if (this._isIdGenerated(model)) {
    tasks.push(
      function (callback) {
        self.query('DROP SEQUENCE ' + seqName, function(err) {
          if (err && err.toString().indexOf('ORA-02289') >= 0) {
            err = null; // Ignore it
          }
          callback(err);
        });
      });
    // Triggers will be dropped as part the drop table
  }
  async.series(tasks, cb);
};

/**
 * Create a table for the given model
 * @param {String} model The model name
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.createTable = function (model, cb) {
  var self = this;
  var name = self.tableEscaped(model);
  var seqName = self.escapeName(model + '_ID_SEQUENCE');
  var triggerName = self.escapeName(model + '_ID_TRIGGER');

  var tasks = [
    function (callback) {
      self.query('CREATE TABLE ' + name + ' (\n  ' + self.propertiesSQL(model) + '\n)', callback);
    }];

  if (this._isIdGenerated(model)) {
    tasks.push(
      function (callback) {
        self.query('CREATE SEQUENCE ' + seqName +
          ' START WITH 1 INCREMENT BY 1 CACHE 100', callback);
      });

    tasks.push(
      function (callback) {
        self.query('CREATE OR REPLACE TRIGGER ' + triggerName +
          ' BEFORE INSERT ON ' + name + ' FOR EACH ROW \n' +
          'BEGIN\n' +
          '  SELECT ' + seqName + '.NEXTVAL INTO :new.' + self.idColumnEscaped(model) + ' FROM dual;\n' +
          'END;', callback);
      });
  }

  async.series(tasks, cb);
};

/**
 * Disconnect from Oracle
 * @param {Function} [cb] The callback function
 */
Oracle.prototype.disconnect = function disconnect(cb) {
  if (this.pool) {
    if (this.settings.debug) {
      this.debug('Disconnecting from ' + this.settings.hostname);
    }
    var pool = this.pool;
    this.pool = null;
    pool.terminate(cb || function() {});
  }
  if (cb) {
    process.nextTick(cb);
  }
};

function isNullable(p) {
  return !(p.required || p.id || p.nullable === false ||
    p.allowNull === false || p['null'] === false);
}

function propertyCanBeNull(model, propName) {
  var p = this._models[model].properties[propName];
  return isNullable(p);
}

function escape(val) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean':
      return (val) ? "'Y'" : "'N'";
    case 'number':
      return val + '';
  }

  if (typeof val === 'object') {
    val = (typeof val.toISOString === 'function')
      ? val.toISOString()
      : val.toString();
  }

  val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function (s) {
    switch (s) {
      case "\0":
        return "\\0";
      case "\n":
        return "\\n";
      case "\r":
        return "\\r";
      case "\b":
        return "\\b";
      case "\t":
        return "\\t";
      case "\x1a":
        return "\\Z";
      case "\'":
        return "''"; // For oracle
      case "\"":
        return s; // For oracle
      default:
        return "\\" + s;
    }
  });
  // return "q'#"+val+"#'";
  return "'" + val + "'";
}

/*!
 * Find the column type for a given model property
 *
 * @param {String} model The model name
 * @param {String} property The property name
 * @returns {String} The column type
 */
Oracle.prototype.columnDataType = function (model, property) {
  var columnMetadata = this.columnMetadata(model, property);
  var colType = columnMetadata && columnMetadata.dataType;
  if (colType) {
    colType = colType.toUpperCase();
  }
  var prop = this._models[model].properties[property];
  if (!prop) {
    return null;
  }
  var colLength = columnMetadata && columnMetadata.dataLength || prop.length;
  if (colType) {
    return colType + (colLength ? '(' + colLength + ')' : '');
  }

  switch (prop.type.name) {
    default:
    case 'String':
    case 'JSON':
      return 'VARCHAR2' + (colLength ? '(' + colLength + ')' : '(1024)');
    case 'Text':
      return 'VARCHAR2' + (colLength ? '(' + colLength + ')' : '(1024)');
    case 'Number':
      return 'NUMBER';
    case 'Date':
      return 'DATE';
    case 'Timestamp':
      return 'TIMESTAMP(3)';
    case 'Boolean':
      return 'CHAR(1)'; // Oracle doesn't have built-in boolean
  }
};

Oracle.prototype.ping = function (cb) {
  this.query('select count(*) as result from user_tables', [], cb);
};

/*!
 * Map oracle data types to json types
 * @param {String} oracleType
 * @param {Number} dataLength
 * @returns {String}
 */
function oracleDataTypeToJSONType(oracleType, dataLength) {
  var type = oracleType.toUpperCase();
  switch (type) {
    case 'CHAR':
      if (dataLength === 1) {
        // Treat char(1) as boolean
        return 'Boolean';
      } else {
        return 'String';
      }
      break;
    case 'VARCHAR':
    case 'VARCHAR2':
    case 'LONG VARCHAR':
    case 'NCHAR':
    case 'NVARCHAR2':
      return 'String';
    case 'LONG':
    case 'BLOB':
    case 'CLOB':
    case 'NCLOB':
      return 'Binary';
    case 'NUMBER':
    case 'INTEGER':
    case 'DECIMAL':
    case 'DOUBLE':
    case 'FLOAT':
    case 'BIGINT':
    case 'SMALLINT':
    case 'REAL':
    case 'NUMERIC':
    case 'BINARY_FLOAT':
    case 'BINARY_DOUBLE':
    case 'UROWID':
    case 'ROWID':
      return 'Number';
    case 'DATE':
    case 'TIMESTAMP':
      return 'Date';
    default:
      return 'String';
  }
}

function mapOracleDatatypes(typeName) {
  //TODO there are a lot of synonymous type names that should go here-- this is just what i've run into so far
  switch (typeName) {
    case 'int4':
      return 'NUMBER';
    case 'bool':
      return 'CHAR(1)';
    default:
      return typeName;
  }
}

function propertyHasNotBeenDeleted(model, propName) {
  return !!this._models[model].properties[propName];
}
require('./migration')(Oracle);
require('./discovery')(Oracle);
