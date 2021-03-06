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

//! Execution plan for reading in-memory batches of data

use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream};
use crate::physical_plan::LambdaExecPlan;
use crate::error::{DataFusionError, Result};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use async_trait::async_trait;
use futures::Stream;

use serde::{Deserialize, Serialize};

/// Execution plan for reading in-memory batches of data
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MemoryExec {
    /// The partitions to query
    #[serde(skip)]
    pub partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data after the optional projection is applied
    pub schema: SchemaRef,
    /// Optional projection
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<Vec<usize>>,
}

#[async_trait]
#[typetag::serde(name = "memory_exec")]
impl ExecutionPlan for MemoryExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn with_new_children(
        &self,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition].clone(),
            self.schema.clone(),
            self.projection.clone(),
        )?))
    }
}

#[async_trait]
impl LambdaExecPlan for MemoryExec {
    fn feed_batches(&mut self, _partitions: Vec<Vec<RecordBatch>>) {
        unimplemented!();
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projection,
        })
    }

    /// Set the partitions and schema
    pub fn set_partitions_and_schema(&mut self, partitions: &Vec<Vec<RecordBatch>>, schema: SchemaRef) {
        self.partitions = partitions.clone();
        self.schema = schema;
    }

    /// Set the partitions
    pub fn set_partitions(&mut self, partitions: &Vec<Vec<RecordBatch>>) {
        self.partitions = partitions.clone();
    }

    /// Get the projection
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }
}

/// Iterator over batches
pub(crate) struct MemoryStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0,
        })
    }
}

impl Stream for MemoryStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            // apply projection
            match &self.projection {
                Some(columns) => Some(RecordBatch::try_new(
                    self.schema.clone(),
                    columns.iter().map(|i| batch.column(*i).clone()).collect(),
                )),
                None => Some(Ok(batch.clone())),
            }
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
