// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DummyExec execution plan

use std::any::Any;
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{ExecutionPlan, LambdaExecPlan, Partitioning};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use super::SendableRecordBatchStream;

use async_trait::async_trait;

use serde::{Deserialize, Serialize};

/// Dummy execution plan
#[derive(Debug, Serialize, Deserialize)]
pub struct DummyExec {}

#[async_trait]
#[typetag::serde(name = "dummy_exec")]
impl ExecutionPlan for DummyExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The filter operator does not make any changes to the schema of its input
        Arc::new(Schema::empty())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(0)
    }

    fn with_new_children(
        &self,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "DummyExec invalid children".to_string(),
        ))
    }

    async fn execute(&self, _: usize) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "DummyExec invalid partition".to_string(),
        ))
    }
}

#[async_trait]
impl LambdaExecPlan for DummyExec {
    fn feed_batches(&mut self, _partitions: Vec<Vec<RecordBatch>>) {
        unimplemented!();
    }
}
