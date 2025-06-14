use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{Array, ArrayRef, StringArray};

use datafusion_python::datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_python::datafusion_expr::{
    create_udaf, Accumulator, AccumulatorFactoryFunction, AggregateUDF, Volatility,
};
use serde_json;

#[derive(Debug, Default)]
struct MeanQHistogram {
    hist: HashMap<u64, u64>,
}

impl MeanQHistogram {
    fn add_read(&mut self, qual_str: &str) {
        let mut sum = 0u64;
        let mut len = 0u64;
        for b in qual_str.bytes() {
            sum += (b.saturating_sub(33)) as u64;
            len += 1;
        }
        if len > 0 {
            let mean = sum / len;
            *self.hist.entry(mean).or_default() += 1;
        }
    }
}

impl Accumulator for MeanQHistogram {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let json = serde_json::to_string(&self.hist)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(vec![ScalarValue::Utf8(Some(json))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.add_read(arr.value(i));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = states[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let sub: HashMap<String, u64> =
                serde_json::from_str(arr.value(i)).map_err(|e| DataFusionError::Execution(e.to_string()))?;
            for (k, v) in sub {
                let key = k.parse::<u64>().map_err(|e| DataFusionError::Execution(e.to_string()))?;
                *self.hist.entry(key).or_default() += v;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let json = serde_json::to_string(&self.hist)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(ScalarValue::Utf8(Some(json)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.hist.len() * std::mem::size_of::<(u64, u64)>()
    }
}

pub fn create_sequence_quality_score_udaf() -> AggregateUDF {
    let input_types = vec![DataType::Utf8];
    let return_type = Arc::new(DataType::Utf8);
    let state_types = Arc::new(vec![DataType::Utf8]);

    let factory: AccumulatorFactoryFunction =
        Arc::new(|_| Ok(Box::<MeanQHistogram>::default()));

    create_udaf(
        "sequence_quality_score",
        input_types,
        return_type,
        Volatility::Immutable,
        factory,
        state_types,
    )
}
