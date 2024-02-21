
output "subnet" {
  value = aws_subnet.public_subnet.id
}

output "security_group" {
  value = aws_security_group.sec_group.id
}

output "ecs_cluster_name" {
  value = local.name
}
