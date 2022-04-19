#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os

import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f"Loading artifact {args.input_artifact}")

    local_path = wandb.use_artifact(args.input_artifact).file()

    df = pd.read_csv(local_path)

    logger.info("Clipping price values...")
    idx = df['price'].between(args.min_price, args.max_price)

    df = df[idx].copy()

    logger.info("Convert last_review to datetime...")
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f"Logging artifact {args.output_artifact}")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    tmp_file = "clean_sample.csv"

    df.to_csv(tmp_file, index=False)

    artifact.add_file(tmp_file)

    run.log_artifact(artifact)

    os.remove(args.output_artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully qualified name for the artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact to create",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact to create",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Details of the produced artifact.",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum accepted price value",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum accepted price value",
        required=True
    )


    args = parser.parse_args()

    go(args)
