# Bucket para resultados do Athena
resource "aws_s3_bucket" "athena_results" {
  bucket = "${local.prefix}-resuts-queries-athena-${var.account}"
  tags   = local.common_tags
  acl    = "private"
}

# Configuração de criptografia para o bucket do Athena
resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results_sse" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # ou "aws:kms" se preferir KMS
    }
  }
}

# Configuração de versionamento para o bucket do Athena
resource "aws_s3_bucket_versioning" "athena_results_versioning" {
  bucket = aws_s3_bucket.athena_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Workgroup do Athena com output location apontando para o bucket recém-criado
# resource "aws_athena_workgroup" "workgroup_athena" {
#   name = "primary"

#   configuration {
#     result_configuration {
#       output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
#     }
#   }

#   force_destroy = true
# }