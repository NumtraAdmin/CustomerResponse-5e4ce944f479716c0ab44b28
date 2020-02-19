import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e4ce944f479716c0ab44b29','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	CustomerResponse_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e4ce944f479716c0ab44b29", spark, "{'url': '/Demo/CustomerResponse.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib3c8e0614707f7e6d2addea6ce7c33d0', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e4ce944f479716c0ab44b29','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e4ce944f479716c0ab44b29','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e4ce944f479716c0ab44b2a','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	CustomerResponse_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e4ce944f479716c0ab44b29"],{"5e4ce944f479716c0ab44b29": CustomerResponse_DBFS}, "5e4ce944f479716c0ab44b2a", spark,json.dumps( {"FE": [{"feature": "Distance_from_nearest_store", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "496", "mean": "5.26", "stddev": "2.91", "min": "0.0661583065558593", "max": "10.0017264678496", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Mosaic_group"}, "feature": "Mosaic_group", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "496", "mean": "", "stddev": "", "min": "A", "max": "S", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Amount_purchased_6m", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "496", "mean": "439.6", "stddev": "307.32", "min": "0.0", "max": "1216.21", "missing": "0"}, "transformation": ""}, {"feature": "Purchased_sale_soda", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "496", "mean": "0.48", "stddev": "0.5", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {"feature_label": "Mosaic_likelihood"}, "feature": "Mosaic_likelihood", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "496", "mean": "", "stddev": "", "min": "Average", "max": "Unlikely", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Mosaic_group_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "5000", "mean": "7.05", "stddev": "5.39", "min": "0.0", "max": "18.0", "missing": "0"}}, {"feature": "Mosaic_likelihood_transform", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "5000", "mean": "0.81", "stddev": "0.76", "min": "0", "max": "2", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e4ce944f479716c0ab44b2a','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e4ce944f479716c0ab44b2a','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e4ce944f479716c0ab44b2b','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	CustomerResponse_AutoML = tpot_execution.Tpot_execution.run(["5e4ce944f479716c0ab44b2a"],{"5e4ce944f479716c0ab44b2a": CustomerResponse_AutoFE}, "5e4ce944f479716c0ab44b2b", spark,json.dumps( {"model_type": "classification", "label": "Purchased_sale_soda", "features": ["Distance_from_nearest_store", "Mosaic_group", "Amount_purchased_6m", "Mosaic_likelihood"], "percentage": "40", "executionTime": 45, "sampling": "0", "sampling_value": "none", "run_id": "a28181d306904deda4011d2dc4220d7f", "model_id": "5e4d1c571bfdaec91f4ce9bf", "ProjectName": "ML Scenarios", "PipelineName": "CustomerResponse", "userid": "567a95c8ca676c1d07d5e3e7", "runid": "", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))

	PipelineNotification.PipelineNotification().completed_notification('5e4ce944f479716c0ab44b2b','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e4ce944f479716c0ab44b2b','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)

