// Licensed to Cloudera, Inc. under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  Cloudera, Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import CancellablePromise from 'api/cancellablePromise';
import { STATUS, ExecutableStatement } from './executableStatement';
import sqlStatementsParser from 'parse/sqlStatementsParser';

class Executor {
  constructor() {
    this.lastExecutables = [];
    this.status = STATUS.ready;
  }

  /**
   * @param options
   * @param {string} options.sourceType
   * @param {ContextCompute} options.compute
   * @param {ContextNamespace} options.namespace
   * @param {string} options.statement
   * @param {string} [options.database]
   */
  executeStatements(options) {
    this.status = STATUS.running;

    const executables = [];
    const deferred = $.Deferred();

    if (options.isSqlDialect) {
      let database = options.database;
      sqlStatementsParser.parse(options.statements).forEach(parsedStatement => {
        // If there's no first token it's a trailing comment
        if (parsedStatement.firstToken) {

          // TODO: Do we want to send USE statements separately or do we want to send database as param instead?
          if (/USE/i.test(parsedStatement.firstToken)) {
            let dbMatch = parsedStatement.statement.match(/use\s+([^;]+)/i);
            if (dbMatch) {
              database = dbMatch[1];
            }
          } else {
            executables.push(new ExecutableStatement({
              sourceType: options.sourceType,
              compute: options.compute,
              namespace: options.namespace,
              database: database,
              parsedStatement: parsedStatement
            }));
          }
        }
      });
    } else {
      executables.push(new ExecutableStatement(options));
    }

    this.lastExecutables = executables.concat(); // Clone

    const cancellablePromises = [{
      cancel: () => {
        const running = executables.filter(executable => executable.status === STATUS.running);
        if (running.length) {
          this.status = STATUS.canceling;
          const cancelPromises = running.map(executable => executable.cancel());
          return $.when(cancelPromises).then(() => {
            this.status = STATUS.canceled;
          });
        }
        return $.Deferred().resolve().promise();
      }
    }];

    const executeNext = () => {
      if (this.status === STATUS.running) {
        if (executables.length) {
          executables.shift().execute().then(executeNext).catch(errorMessage => {
            this.status = STATUS.failed;
          })
        } else {
          this.status = STATUS.success;
          deferred.resolve();
        }
      }
    };

    executeNext();

    return new CancellablePromise(deferred, undefined, cancellablePromises);
  }
}

export default Executor;