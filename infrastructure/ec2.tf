resource "aws_security_group" "airflow" {
  name        = "airflow-sg"
  description = "Libera acesso ao Airflow e SSH"

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_eip" "airflow" {
  domain = "vpc"
}

resource "aws_instance" "airflow" {
  ami           = "ami-0c55b159cbfafe1f0" # Amazon Linux 2
  instance_type = "t2.medium"
  key_name      = "key-pair-airflow-tech-challanger-3"

  user_data = <<-EOF
    #!/bin/bash
    yum update -y
    amazon-linux-extras install docker -y
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user
  EOF

  vpc_security_group_ids = [aws_security_group.airflow.id]
}

resource "aws_eip_association" "airflow" {
  instance_id   = aws_instance.airflow.id
  allocation_id = aws_eip.airflow.id
}

resource "aws_route53_record" "airflow" {
  zone_id = "ZONA_DNS_AWS"
  name    = "airflow.fiapmlet.com"
  type    = "A"
  ttl     = 300
  records = [aws_eip.airflow.public_ip]
}
