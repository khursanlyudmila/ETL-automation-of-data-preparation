/* Урок 4. Партицирование данных по дате. Динамическое партицирование
1. Создайте таблицу movies с полями movies_type, director, year_of_issue, length_in_minutes, rate.

2. Сделайте таблицы для горизонтального партицирования по году выпуска (до 1990, 1990 -2000, 2000- 2010, 2010-2020, после 2020).

3. Сделайте таблицы для горизонтального партицирования по длине фильма (до 40 минута, от 40 до 90 минут, от 90 до 130 минут, более 130 минут).

4. Сделайте таблицы для горизонтального партицирования по рейтингу фильма (ниже 5, от 5 до 8, от 8до 10).

5. Создайте правила добавления данных для каждой таблицы.

6. Добавьте фильмы так, чтобы в каждой таблице было не менее 3 фильмов.

7. Добавьте пару фильмов с рейтингом выше 10.

8. Сделайте выбор из всех таблиц, в том числе из основной.

9. Сделайте выбор только из основной таблицы.*/


CREATE TABLE if not exists movies (
id bigint not null primary key,
movies_type character varying,
director character varying not NULL,
year_of_issue int not NULL,
length_in_minutes real not NULL,
rate int not null
);

create table if not exists year_before_1990 (check (year_of_issue < 1990)) INHERITS (movies);
create table if not exists year_1990_2000 (check (year_of_issue >= 1990 and year_of_issue < 2000)) INHERITS (movies);
create table if not exists year_2000_2010 (check (year_of_issue >= 2000 and year_of_issue < 2010)) INHERITS (movies);
create table if not exists year_2010_2020 (check (year_of_issue >= 2010 and year_of_issue < 2020)) INHERITS (movies);
create table if not exists year_after_2020 (check (year_of_issue >= 2020)) INHERITS (movies);

create rule insert_year_before_1990 as on insert to movies
where (year_of_issue < 1990) do instead insert into year_before_1990 values (new.*);

create rule insert_year_1990_2000 as on insert to movies
where ((year_of_issue >= 1990) and (year_of_issue < 2000)) do instead insert into year_1990_2000 values (new.*);

create rule insert_year_2000_2010 as on insert to movies
where ((year_of_issue >= 2000) and (year_of_issue < 2010)) do instead insert into year_2000_2010 values (new.*);

create rule insert_year_2010_2020 as on insert to movies
where ((year_of_issue >= 2010) and (year_of_issue < 2020)) do instead insert into year_2010_2020 values (new.*);

create rule insert_year_after_2020 as on insert to movies
where (year_of_issue >= 2020) do instead insert into year_after_2020 values (new.*);

create table if not exists length_in_minutes_before_40 (check (length_in_minutes < 40)) INHERITS (movies);
create table if not exists length_in_minutes_40_90 (check (length_in_minutes >= 40 and length_in_minutes < 90)) INHERITS (movies);
create table if not exists length_in_minutes_90_130 (check (length_in_minutes >= 90 and length_in_minutes < 130)) INHERITS (movies);
create table if not exists length_in_minutes_after_130 (check (length_in_minutes >= 130)) INHERITS (movies);

create rule insert_length_in_minutes_before_40 as on insert to movies
where (length_in_minutes < 40) do instead insert into length_in_minutes_before_40 values (new.*);

create rule insert_length_in_minutes_40_90 as on insert to movies
where ((length_in_minutes >= 40) and (length_in_minutes < 90)) do instead insert into length_in_minutes_40_90 values (new.*);

create rule insert_length_in_minutes_90_130 as on insert to movies
where ((length_in_minutes >= 90) and (length_in_minutes < 130)) do instead insert into length_in_minutes_90_130 values (new.*);

create rule insert_length_in_minutes_after_130 as on insert to movies
where (length_in_minutes >= 130) do instead insert into length_in_minutes_after_130 values (new.*);

create table if not exists rate_before_5 (check (rate < 5)) INHERITS (movies);
create table if not exists rate_5_8 (check (rate >= 5 and rate < 8)) INHERITS (movies);
create table if not exists rate_8_10 (check (rate >= 8 and rate <= 10)) INHERITS (movies);

create rule insert_rate_before_5 as on insert to movies
where (rate < 5) do instead insert into rate_before_5 values (new.*);

create rule insert_rate_5_8 as on insert to movies
where ((rate >= 5) and (rate < 8)) do instead insert into rate_5_8 values (new.*);

create rule insert_rate_8_10 as on insert to movies
where ((rate >= 8) and (rate <= 10)) do instead insert into rate_8_10 values (new.*);

INSERT INTO movies (id, movies_type, director, year_of_issue, length_in_minutes, rate)
VALUES (1, 'comedy', 'A. Ivanov', 1984, 98, 4),
(2, 'honor', 'S. Petrov', 2015, 132, 5),
(3, 'detective', 'S. White', 2005, 35, 6),
(4, 'thriller', 'J. Black', 2023, 65, 3),
(5, 'action movie', 'M. Sidorov', 2015, 98, 11),
(6, 'comedy', 'A. Shuvalov', 1991, 25, 7),
(7, 'honor', 'W. Scotch', 2018, 100, 8),
(8, 'detective', 'S. White', 2005, 35, 7),
(9, 'thriller', 'A. Strike', 2021, 125, 12),
(10, 'action movie', 'Z. Quite', 1999, 78, 7),
(11, 'comedy', 'N. Good', 2013, 134, 9),
(12, 'honor', 'O. Peace', 2009, 38, 4),
(13, 'detective', 'P. Peater', 2004, 78, 6),
(14, 'thriller', 'A. Strike', 2021, 125, 10),
(15, 'action movie', 'H. Yellow', 2024, 104, 9)

SELECT * FROM year_before_1990;
SELECT * FROM year_1990_2000;
SELECT * FROM year_2000_2010;
SELECT * FROM year_2010_2020;
SELECT * FROM year_after_2020;

SELECT * FROM length_in_minutes_before_40;
SELECT * FROM length_in_minutes_40_90;
SELECT * FROM length_in_minutes_90_130;
SELECT * FROM length_in_minutes_after_130;

SELECT * FROM rate_before_5;
SELECT * FROM rate_5_8;
SELECT * FROM rate_8_10;

SELECT * FROM movies;

SELECT * FROM ONLY movies;




