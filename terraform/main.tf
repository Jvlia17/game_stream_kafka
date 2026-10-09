
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region  = "ca-central-1"
  profile = "julia-aws"
}

resource "aws_s3_bucket" "raw" {
  bucket = "gamestream-raw-julia-2026"

  tags = {
    Project     = "GameStream"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
