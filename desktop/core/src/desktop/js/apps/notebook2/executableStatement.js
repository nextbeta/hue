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

import apiHelper from 'api/apiHelper';
import hueAnalytics from "utils/hueAnalytics";

const STATUS = {
  canceled: 'canceled',
  canceling: 'canceling',
  ready: 'ready',
  running: 'running',
  success: 'success',
  failed: 'failed',
};

class ExecutableStatement {

  /**
   * @param options
   * @param {string} options.sourceType
   * @param {ContextCompute} options.compute
   * @param {ContextNamespace} options.namespace
   * @param {string} options.statement
   * @param {string} [options.database]
   */
  constructor(options) {
    this.compute = options.compute;
    this.database = options.database;
    this.namespace = options.namespace;
    this.sourceType = options.sourceType;
    this.statement = options.statement;

    this.lastCancellable = undefined;
    this.status = STATUS.ready;
  }

  execute() {
    if (this.status === STATUS.running) {
      return;
    }
    this.status = STATUS.running;

    hueAnalytics.log('notebook', 'execute/' + this.sourceType);

    const cancellable = apiHelper.execute({
      executable: this
    }).done(response => {
      console.log(response);
    }).fail(error => {
      this.status = STATUS.failed;
    });

    return cancellable;
  }

  cancel() {
    if (this.lastCancellable && this.status === STATUS.running) {
      hueAnalytics.log('notebook', 'cancel/' + this.sourceType);
      this.status = STATUS.canceling;
      this.lastCancellable.cancel().always(() => {
        this.status = STATUS.canceled;
      });
      this.lastCancellable = undefined;
    }
  }
}

export { STATUS, ExecutableStatement };