use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{Array, ArrayRef, StringArray};
use datafusion_python::datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_python::datafusion_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

use serde_json;

pub fn create_quality_score_histogram_udf() -> ScalarUDF {
    let func = |args: &[ColumnarValue]| -> Result<ColumnarValue> {
        let string_array = match &args[0] {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?,
            ColumnarValue::Scalar(_) => {
                return Err(DataFusionError::Execution(
                    "Expected array input, got scalar".to_string(),
                ))
            }
        };

        let mut results = Vec::with_capacity(string_array.len());

        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                results.push(ScalarValue::Utf8(None));
            } else {
                let s = string_array.value(i);
                let mut histogram = HashMap::<u64, u64>::new();

                for ch in s.chars() {
                    let q = (ch as u8).saturating_sub(33) as u64;
                    *histogram.entry(q).or_insert(0) += 1;
                }

                let json = serde_json::to_string(&histogram)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                results.push(ScalarValue::Utf8(Some(json)));
            }
        }

        let array = ScalarValue::iter_to_array(results.into_iter())?;
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "quality_score_histogram",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}
