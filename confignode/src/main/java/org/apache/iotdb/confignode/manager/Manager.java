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
 */
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

/**
 * a subset of services provided by {@ConfigManager}. For use internally only, passed to Managers,
 * services.
 */
public interface Manager {

  /**
   * if a service stop
   *
   * @return true if service stopped
   */
  boolean isStopped();

  /**
   * Get DataManager
   *
   * @return DataNodeManager instance
   */
  DataNodeManager getDataNodeManager();

  /**
   * Get ConsensusManager
   *
   * @return ConsensusManager instance
   */
  ConsensusManager getConsensusManager();

  /**
   * Get ClusterSchemaManager
   *
   * @return ClusterSchemaManager instance
   */
  ClusterSchemaManager getClusterSchemaManager();

  /**
   * Get PartitionManager
   *
   * @return PartitionManager instance
   */
  PartitionManager getPartitionManager();

  /**
   * Register DataNode
   *
   * @param configRequest RegisterDataNodePlan
   * @return DataNodeConfigurationDataSet
   */
  DataSet registerDataNode(ConfigRequest configRequest);

  /**
   * Get DataNode info
   *
   * @param configRequest QueryDataNodeInfoPlan
   * @return DataNodesInfoDataSet
   */
  DataSet getDataNodeInfo(ConfigRequest configRequest);

  /**
   * Get StorageGroupSchemas
   *
   * @return StorageGroupSchemaDataSet
   */
  DataSet getStorageGroupSchema();

  /**
   * Set StorageGroup
   *
   * @param configRequest SetStorageGroupReq
   * @return status
   */
  TSStatus setStorageGroup(ConfigRequest configRequest);

  /**
   * Get SchemaPartition
   *
   * @return SchemaPartitionDataSet
   */
  DataSet getSchemaPartition(PathPatternTree patternTree);

  /**
   * Get or create SchemaPartition
   *
   * @return SchemaPartitionDataSet
   */
  DataSet getOrCreateSchemaPartition(PathPatternTree patternTree);

  /**
   * Get DataPartition
   *
   * @param configRequest DataPartitionPlan
   * @return DataPartitionDataSet
   */
  DataSet getDataPartition(ConfigRequest configRequest);

  /**
   * Get or create DataPartition
   *
   * @param configRequest DataPartitionPlan
   * @return DataPartitionDataSet
   */
  DataSet getOrCreateDataPartition(ConfigRequest configRequest);

  /**
   * Operate Permission
   *
   * @param configRequest AuthorPlan
   * @return status
   */
  TSStatus operatePermission(ConfigRequest configRequest);

  /**
   * Query Permission
   *
   * @param configRequest AuthorPlan
   * @return PermissionInfoDataSet
   */
  DataSet queryPermission(ConfigRequest configRequest);
}