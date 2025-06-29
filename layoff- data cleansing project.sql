select * from layoffs ; 

-- i am checking whether there is any duplicate in the layoff table
-- when a row is more than one time it indicates that there is duplicates so i want to delete those unwanted dups

with duplicate_cte as (
select *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
from layoffs 
)
select *
from duplicate_cte 
where row_num >1;

-- so i am creating new table like the layoffs so i can delete and test them

CREATE TABLE staging_layoffs (
    company TEXT,
    location TEXT,
    total_laid_off INTEGER DEFAULT NULL,
    percentage_laid_off TEXT,
    dates TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INTEGER DEFAULT NULL
);

-- again checking once 
CREATE TABLE IF NOT EXISTS staging_layoffs (
    company TEXT,
    location TEXT,
    total_laid_off INTEGER DEFAULT NULL,
    percentage_laid_off TEXT,
    dates TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INTEGER DEFAULT NULL
);

SELECT * FROM staging_layoffs;

-- i am inserting the old data into new table

INSERT INTO staging_layoffs (
    company,
    location,
    total_laid_off,
    percentage_laid_off,
    dates,
    stage,
    country,
    funds_raised_millions
)
SELECT
    company,
    location,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions
FROM layoffs;

ALTER TABLE staging_layoffs ADD COLUMN row_num INTEGER;


with duplicates_cte as (
select *,
ROW_NUMBER() OVER(PARTITION BY company, location, total_laid_off, percentage_laid_off, 'dates', stage, country, funds_raised_millions ) AS row_num
from staging_layoffs  
)
select *
from duplicates_cte 
where row_num >1;

insert into staging_layoffs
select *,
ROW_NUMBER() OVER(PARTITION BY company, location, total_laid_off, percentage_laid_off, 'dates', stage, country, funds_raised_millions ) AS row_num
from layoffs 

Delete
from duplicates_cte 
where row_num >1;

--we can delete the unwanted rows by this.

-- WE WILL GO TO STRANDARDIZING DATA

select company, trim(company)
from staging_layoffs

update staging_layoffs 
set company = trim(company)

select distinct industry
from staging_layoffs
order by 1
--we check if there is any industry which is similar. making all industries starting with cryto as crypto
select *
from staging_layoffs
where industry like 'Crypto%'

update staging_layoffs sl 
set industry ='Crypto'
where industry like 'Crypto%'

select distinct location 
from staging_layoffs
order by 1

select distinct country 
from staging_layoffs
order by 1

-- one united states has . at end of it

select *
from staging_layoffs
where country like 'United states%'
order by 1

select distinct country, TRIM(Trailing '.' from country) 
from staging_layoffs
order by 1

SELECT DISTINCT 
    country, 
    TRIM(TRAILING '.' FROM country) AS trimmed_country
FROM staging_layoffs
ORDER BY country;
