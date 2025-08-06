CREATE database lab_distribuidos;

use lab_distribuidos;

create table metrics(
id int auto_increment primary key,
ip varchar (45),
cpu float,
mem float,
timestamp datetime
);

CREATE USER 'joseph'@'%' IDENTIFIED BY 'joseph';
GRANT ALL PRIVILEGES ON lab_distribuidos.* TO 'joseph'@'%';
FLUSH PRIVILEGES;

select*from metrics;
truncate table metrics;