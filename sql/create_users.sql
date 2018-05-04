CREATE TABLE `users` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(11) DEFAULT NULL,
  `is_target` tinyint(1) DEFAULT NULL,
  `sk_id` varchar(11) DEFAULT NULL,
  `wp_id` varchar(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) 