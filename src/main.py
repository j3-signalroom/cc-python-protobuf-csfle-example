from __future__ import annotations
import sys
from dotenv import load_dotenv

from utilities import setup_logging, get_config, parse_args
from schema_registry_client import SchemaRegistryClient
from kafka_helpers import ensure_topics
from examples import (
    example_basic,
    example_delete_protection,
    example_evolution,
    example_oneof,
    example_null_handling,
    example_compatibility,
    example_types,
    example_strategies,
    example_csfle,
    example_no_auto_register,
)


__copyright__  = "Copyright (c) 2026 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def main() -> None:
    # Setup module logging
    logger = setup_logging()

    # loads .env from cwd (or any parent directory) into os.environ
    load_dotenv()  

    # Parse command-line args and config from environment variables
    args = parse_args()

    # Check for required config and exit with instructions if any are missing
    cfg, missing = get_config()

    sr_missing = [m for m in missing if m in
                  ("SCHEMA_REGISTRY_URL", "SR_API_KEY", "SR_API_SECRET")]
    if sr_missing:
        logger.error("ERROR: Missing Schema Registry credentials:")
        for m in sr_missing:
            logger.error(f"  export {m}=<value>")
        sys.exit(1)

    kafka_missing = [m for m in missing if m in
                     ("BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET")]
    if args.mode == "full" and kafka_missing:
        logger.error("ERROR: --mode full requires Kafka credentials:")
        for m in kafka_missing:
            logger.error(f"  export {m}=<value>")
        sys.exit(1)

    csfle_needed = args.example in ("all", "csfle")
    if csfle_needed and "AWS_KMS_KEY_ARN" in missing:
        logger.error("ERROR: CSFLE demo requires AWS KMS key ARN:")
        logger.error("  export AWS_KMS_KEY_ARN=arn:aws:kms:region:acct:key/key-id")
        sys.exit(1)

    run_id     = args.run_id
    kafka_cfg  = cfg if args.mode == "full" else None
    save_dir   = args.save_schemas or ""
    use_protoc = args.use_protoc
    sr         = SchemaRegistryClient(cfg["sr_url"], cfg["sr_api_key"], cfg["sr_api_secret"])

    # Compile .proto files if --use-protoc is set
    if use_protoc:
        from compiled_protobuf_helpers import compile_protos
        logger.info("\n[protoc] Compiling .proto schemas …")
        compile_protos()

    logger.info("=" * 100)
    logger.info("  Confluent Cloud Python Protobuf CSFLE (Client-Side Field-Level Encryption) Examples")
    proto_mode = "protoc (compiled stubs)" if use_protoc else "dynamic (runtime descriptors)"
    logger.info(f"  Python {sys.version.split()[0]}  |  mode={args.mode}  |  run_id={run_id}  |  protobuf={proto_mode}")
    logger.info(f"  SR:    {cfg['sr_url']}")
    if kafka_cfg:
        logger.info(f"  Kafka: {cfg['bootstrap_servers']}")
    if save_dir:
        logger.info(f"  Schemas → {save_dir}")
    logger.info("=" * 100)

    # Pre-create all topics required by the demos before any produce calls
    if kafka_cfg:
        required_topics = [
            f"testproto-{run_id}",
            f"transactions-proto-{run_id}",
            f"all-events-{run_id}",
            f"nullables-{run_id}",
            f"payments-{run_id}",
            f"csfle-{run_id}",
            f"invoices-{run_id}",
        ]
        logger.info("\n[Admin] Ensuring topics exist …")
        ensure_topics(kafka_cfg, required_topics)

    run_all = args.example == "all"

    if run_all or args.example == "basic":
        example_basic(sr, kafka_cfg, run_id, save_dir, use_protoc)

    if run_all or args.example == "delete":
        example_delete_protection(sr, run_id)

    if run_all or args.example == "evolution":
        example_evolution(sr, kafka_cfg, run_id, save_dir, use_protoc)

    if run_all or args.example == "oneof":
        example_oneof(sr, kafka_cfg, run_id, save_dir, use_protoc)

    if run_all or args.example == "null":
        example_null_handling(sr, run_id, save_dir, use_protoc)

    if run_all or args.example == "compat":
        example_compatibility(sr)

    if run_all or args.example == "types":
        example_types(sr)

    if run_all or args.example == "strategies":
        example_strategies(sr, run_id, save_dir, use_protoc)

    if (run_all and args.mode == "full") or args.example == "csfle":
        example_csfle(sr, kafka_cfg, run_id, cfg.get("aws_kms_key_arn", ""), save_dir, use_protoc)

    if run_all or args.example == "no-auto-register":
        example_no_auto_register(sr, kafka_cfg, run_id, save_dir, use_protoc)

    logger.info(f"\n{'─' * 100}")
    logger.info(f"  Done. All topics/subjects use suffix '-{run_id}'.")
    logger.info("  To clean up subjects in Confluent Cloud CLI:")
    logger.info(f"    confluent schema-registry subject list | grep '{run_id}' | \\")
    logger.info("      xargs -I{} confluent schema-registry subject delete --subject {} --force")
    logger.info(f"{'─' * 70}\n")


if __name__ == "__main__":
    main()
