CREATE TABLE `requests` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `users_id` int(11) DEFAULT NULL,
  `count` int(11) DEFAULT NULL,
  `request_date` date DEFAULT NULL,
  PRIMARY KEY (`id`)
)