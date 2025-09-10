{
  "type": "kafka",
  "id": "cdn-data",
  "spec": {
    "dataSchema": {
      "dataSource": "cdn-data",
      "timestampSpec": {
        "column": "Seizure Time",
        "format": "auto",
        "missingValue": null
      },
      "dimensionsSpec": {
        "dimensions": [
          {
            "type": "string",
            "name": "Record Type",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Calling Number",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Called Number",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Recording Entity",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "MSC Incoming Route",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "MSC Outgoing Route",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Answer Time",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Release Time",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "long",
            "name": "Call Duration",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "string",
            "name": "Diagnostics",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Call Reference",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Basic Service",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Roaming Number",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "MSC Outgoing Circuit",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Originating MSC ID",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Filename",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Parsed Time",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Operator",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "IMSI",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "IMEI",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "MSISDN",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "MS Classmark",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Location Area Code (LAC)",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Cell Identifier",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "System Type",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Basic Service Type",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Basic Service Code",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Charged Party",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Originating RNC/BSC ID",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "long",
            "name": "Record Number",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "string",
            "name": "Translated Number",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Call Type",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "MSC Address",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          }
        ],
        "dimensionExclusions": [
          "__time",
          "Seizure Time"
        ],
        "includeAllDimensions": false,
        "useSchemaDiscovery": false
      },
      "metricsSpec": [],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "HOUR",
        "queryGranularity": {
          "type": "none"
        },
        "rollup": false,
        "intervals": []
      },
      "transformSpec": {
        "filter": null,
        "transforms": []
      }
    },
    "ioConfig": {
      "topic": "cdn-data",
      "topicPattern": null,
      "inputFormat": {
        "type": "json"
      },
      "replicas": 1,
      "taskCount": 8,
      "taskDuration": "PT1800S",
      "consumerProperties": {
        "bootstrap.servers": "broker-1:29092"
      },
      "autoScalerConfig": null,
      "lagAggregator": {
        "type": "default"
      },
      "pollTimeout": 100,
      "startDelay": "PT5S",
      "period": "PT30S",
      "useEarliestOffset": true,
      "completionTimeout": "PT1800S",
      "lateMessageRejectionPeriod": null,
      "earlyMessageRejectionPeriod": null,
      "lateMessageRejectionStartDateTime": null,
      "configOverrides": null,
      "idleConfig": null,
      "stopTaskCount": null,
      "emitTimeLagMetrics": false,
      "stream": "cdn-data",
      "useEarliestSequenceNumber": true
    },
    "tuningConfig": {
      "type": "kafka",
      "appendableIndexSpec": {
        "type": "onheap",
        "preserveExistingMetrics": false
      },
      "maxRowsInMemory": 500000,
      "maxBytesInMemory": 0,
      "skipBytesInMemoryOverheadCheck": false,
      "maxRowsPerSegment": 1000000,
      "maxTotalRows": null,
      "intermediatePersistPeriod": "PT30M",
      "maxPendingPersists": 2,
      "indexSpec": {
        "bitmap": {
          "type": "roaring"
        },
        "dimensionCompression": "lz4",
        "stringDictionaryEncoding": {
          "type": "utf8"
        },
        "metricCompression": "lz4",
        "longEncoding": "longs"
      },
      "indexSpecForIntermediatePersists": {
        "bitmap": {
          "type": "roaring"
        },
        "dimensionCompression": "lz4",
        "stringDictionaryEncoding": {
          "type": "utf8"
        },
        "metricCompression": "lz4",
        "longEncoding": "longs"
      },
      "reportParseExceptions": false,
      "handoffConditionTimeout": 900000,
      "resetOffsetAutomatically": false,
      "segmentWriteOutMediumFactory": null,
      "workerThreads": null,
      "chatRetries": 8,
      "httpTimeout": "PT10S",
      "shutdownTimeout": "PT80S",
      "offsetFetchPeriod": "PT30S",
      "intermediateHandoffPeriod": "P2147483647D",
      "logParseExceptions": false,
      "maxParseExceptions": 1000,
      "maxSavedParseExceptions": 0,
      "numPersistThreads": 4,
      "maxColumnsToMerge": -1,
      "skipSequenceNumberAvailabilityCheck": false,
      "repartitionTransitionDuration": "PT120S"
    }
  },
  "context": null,
  "suspended": false
}