/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.OperatePipePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class DropPipeOperator extends Operator {
  private String pipeName;

  public DropPipeOperator(String pipeName) {
    super(SQLConstant.TOK_DROP_PIPE);
    this.operatorType = OperatorType.DROP_PIPE;

    this.pipeName = pipeName;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new OperatePipePlan(pipeName, operatorType);
  }
}