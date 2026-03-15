from __future__ import annotations
import tomllib
from pathlib import Path
import logging
import logging.config
import argparse
import textwrap
import uuid
import os

from constants import (DEFAULT_TOOL_LOG_FILE, DEFAULT_TOOL_LOG_FORMAT)


__copyright__  = "Copyright (c) 2026 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def setup_logging(log_file: str = DEFAULT_TOOL_LOG_FILE) -> logging.Logger:
    """Load logging configuration from pyproject.toml.  If not found, use default logging.
    
    Arg(s):
        log_file (str): The log file name to use if no configuration is found.
        
    Return(s):
        logging.Logger: Configured logger instance.
    """
    pyproject_path = Path("pyproject.toml")
    
    if pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)
        
        # Extract logging config
        logging_config = config.get("tool", {}).get("logging", {})
        
        if logging_config:
            logging.config.dictConfig(logging_config)
        else:
            # Fallback to basic file logging
            logging.basicConfig(
                level=logging.INFO,
                format=DEFAULT_TOOL_LOG_FORMAT,
                filemode="w",  # This will reset the log file
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
    else:
        # Default logging setup if no pyproject.toml
        logging.basicConfig(
            level=logging.INFO,
            format=DEFAULT_TOOL_LOG_FORMAT,
            filemode="w",  # This will reset the log file
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    return logging.getLogger()


def get_config() -> tuple[dict, list[str]]:
    keys = {
        "bootstrap_servers": "BOOTSTRAP_SERVERS",
        "kafka_api_key":     "KAFKA_API_KEY",
        "kafka_api_secret":  "KAFKA_API_SECRET",
        "sr_url":            "SCHEMA_REGISTRY_URL",
        "sr_api_key":        "SR_API_KEY",
        "sr_api_secret":     "SR_API_SECRET",
        "aws_kms_key_arn":   "AWS_KMS_KEY_ARN",
    }
    cfg, missing = {}, []
    for key, env in keys.items():
        val = os.environ.get(env, "")
        (cfg if val else missing).__setitem__(key, val) if val else missing.append(env)
    return cfg, missing


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Confluent Cloud Python Dynamic or Precompiled Protobuf Example Demos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            flags:
              Required:
                --mode=<MODE>               Run mode: "schema-only" or "full" (default: schema-only)
                --demo=<DEMO>               Demo to run: all, basic, delete, evolution, oneof,
                                            null, compat, types, strategies, csfle,
                                            no-auto-register (default: all)
                --schema-registry-url=<URL> Confluent Schema Registry endpoint URL
                --sr-api-key=<KEY>          Schema Registry API key
                --sr-api-secret=<SECRET>    Schema Registry API secret

              Required for --mode=full:
                --bootstrap-servers=<SERVERS>  Kafka bootstrap servers
                --kafka-api-key=<KEY>          Kafka cluster API key
                --kafka-api-secret=<SECRET>    Kafka cluster API secret

              Required for --demo=csfle or --demo=all:
                --profile=<AWS_SSO_PROFILE> AWS SSO profile for KMS key provisioning

              Optional:
                --run-id=<RUN_ID>           Unique suffix for topic/subject names to prevent
                                            collisions (default: random 8-char UUID)
                --save-schemas=<DIR>        Save generated .proto schemas to DIR, created if
                                            needed (default: disabled)
                --use-protoc                Use protoc-compiled stubs instead of dynamic
                                            runtime Protobuf; requires protoc on PATH

            examples:
              Quick start (schema-only):
                ./run-demo.sh --mode=schema-only --demo=all \\
                  --profile=<AWS_SSO_PROFILE> --schema-registry-url=<URL> \\
                  --sr-api-key=<KEY> --sr-api-secret=<SECRET>

              With Kafka:
                ./run-demo.sh --mode=full --demo=all \\
                  --profile=<AWS_SSO_PROFILE> --schema-registry-url=<URL> \\
                  --sr-api-key=<KEY> --sr-api-secret=<SECRET> \\
                  --bootstrap-servers=<SERVERS> --kafka-api-key=<KEY> \\
                  --kafka-api-secret=<SECRET>

              Single demo:
                ./run-demo.sh --mode=schema-only --demo=evolution \\
                  --profile=<AWS_SSO_PROFILE> --schema-registry-url=<URL> \\
                  --sr-api-key=<KEY> --sr-api-secret=<SECRET>

              Save .proto schemas to disk:
                ./run-demo.sh --mode=schema-only --demo=all --save-schemas=./schemas \\
                  --profile=<AWS_SSO_PROFILE> --schema-registry-url=<URL> \\
                  --sr-api-key=<KEY> --sr-api-secret=<SECRET>

              Use precompiled Protobuf stubs:
                ./run-demo.sh --mode=schema-only --demo=basic --use-protoc \\
                  --schema-registry-url=<URL> --sr-api-key=<KEY> \\
                  --sr-api-secret=<SECRET>
        """),
    )
    p.add_argument("--mode", choices=["schema-only", "full"], default="schema-only")
    p.add_argument(
        "--demo",
        choices=["all", "basic", "delete", "evolution", "oneof",
                 "null", "compat", "types", "strategies", "csfle",
                 "no-auto-register"],
        default="all",
    )
    p.add_argument(
        "--run-id",
        default=str(uuid.uuid4())[:8],
        help="Unique suffix for topic/subject names (prevents collisions). Default: random.",
    )
    p.add_argument(
        "--save-schemas",
        metavar="DIR",
        default="",
        help="Save generated .proto schemas to DIR (created if needed). Default: disabled.",
    )
    p.add_argument(
        "--use-protoc",
        action="store_true",
        default=False,
        help="Use protoc-compiled stubs instead of dynamic runtime Protobuf. "
             "Requires protoc on PATH (brew install protobuf).",
    )
    return p.parse_args()
