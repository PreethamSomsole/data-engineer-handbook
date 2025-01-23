/*
1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.
    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
*/

CREATE TYPE public.film AS 
(
	film text,
	votes integer,
	rating real,
	filmid text
);

CREATE TABLE public.actors
(
	actor_id text,
	actor text,
	films public.film[],
	quality_class text,
	is_active boolean
);


/*
2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.
*/
INSERT INTO public.actors
with yesterday as (
select actorid, actor, films, quality_class, is_active 
from public.actors af  where current_year = 1972
),
today as (
select
	actorid, 
	actor, 
	array_agg(row(af.film, af.votes, af.rating, af.filmid)::film) as films, 
	year, 
	case when avg(af.rating) > 8 then 'star'
		when avg(af.rating) > 7 and avg(af.rating) <= 8 then 'good'
		when avg(af.rating) > 6 and avg(af.rating) <= 7 then 'average'
		when avg(af.rating) <= 6 then 'bad'
	end as quality_class, 
	true as is_active 
from public.actor_films af 
where year = 1973
GROUP BY actorid, actor, year
)
select coalesce(t.actorid, y.actorid) as actorid ,
coalesce(t.actor, y.actor) as actor ,
coalesce(y.films, array[]::film[]) || case when t.films is not null then 
t.films else array[]::film[] end as films,
case when t.quality_class is not null then t.quality_class else y.quality_class end as quality_class,
t.year is not null as is_active, 
1973 as current_year
from today t 
full outer join yesterday y on t.actorid = y.actorid
;
/*
3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.
*/
CREATE TABLE public.actors_history_scd
(
	actorid text,
	actor text,
	quality_class text,
	is_active boolean,
	start_year int,
	end_year int,
    is_current boolean,
    primary key (actorid, start_year)
); 
 
 /*
 4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
*/
with previous as (
select
    a.actorid,
    a.actor, 
    quality_class,
    is_active,
    current_year as year,
    LAG(a.quality_class, 1) over (partition by a.actorid order by a.current_year) as prev_quality_class,
    LAG(a.is_active, 1) over (partition by a.actorid order by a.current_year) as prev_is_active
from 
    public.actors a
), indicators as (
select
    *, 
    case when quality_class != prev_quality_class or is_active != prev_is_active then 1 else 0 end as change_indicator
from previous
),
streaks as (
select *, sum(change_indicator) over (partition by actorid order by year) as streak_identifier 
from indicators
)
select actorid, actor, quality_class, is_active, min(year) as start_date, max(year) as end_date, true as is_current
from streaks
group by actorid, actor, streak_identifier, quality_class, is_active
order by actorid, start_date
;
/*
5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.
*/

CREATE TYPE scd_type AS (
                    quality_class text,
                    is_active boolean,
                    start_year INTEGER,
                    end_year INTEGER
                        )

WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 2021
    AND end_year = 2021
),
     historical_scd AS (
        SELECT
            actorid,
               actor,
               quality_class,
               is_active,
               start_year,
               end_year
        FROM actors_history_scd
        WHERE current_year = 2021
        AND end_year < 2021
     ),
     this_year_data AS (
         SELECT * FROM actors
         WHERE current_year = 2022
     ),
     unchanged_records AS (
         SELECT
                ts.actorid,
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_year,
                ts.current_year as end_year
        FROM this_year_data ts
        JOIN last_year_scd ls
        ON ls.actorid = ts.actorid
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.actorid,
                ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_year,
                        ls.end_year
                        )::scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.start_year,
                        ts.end_year
                        )::scd_type
                ]) as records
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actorid = ts.actorid
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT actorid,
                actor,
                (records::scd_type).quality_class,
                (records::scd_type).is_active,
                (records::scd_type).start_year,
                (records::scd_type).end_year
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.actorid,
            ts.actor,
                ts.quality_class,
                ts.is_active,
                ts.current_year AS start_year,
                ts.current_year AS end_year
         FROM this_year_data ts
         LEFT JOIN last_year_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2022 AS current_year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a