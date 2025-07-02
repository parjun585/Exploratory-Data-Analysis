-- Data Cleaning 
-- Dataset Link: https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_stagging 
SELECT * FROM world_layoffs.layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



SELECT *
FROM world_layoffs.layoff_stagging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoff_stagging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoff_stagging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM world_layoffs.layoff_stagging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoff_stagging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

CREATE TABLE `world_layoffs`.`layoff_stagging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoff_stagging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoff_stagging;

-- now that we have this we can delete rows were row_num is greater than 2
select * from layoff_stagging2
WHERE row_num >= 2;

DELETE FROM layoff_stagging2
WHERE row_num >= 2;

select * from layoff_stagging2
WHERE row_num >= 2; # now the duplicates are deleted 



-- The other way is to approach it like this.
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_stagging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Standardizing Data 
-- the name of the company has some extra spaces inside that so trimming the dat a

select company, trim(company) from layoff_stagging2;

select DISTINCT industry
from layoff_stagging2
order by 1;

-- while looking at the data we can see that  crypto is written as different forms and some have null values too

select * from layoff_stagging2
where industry is NULL;

Select * from layoff_stagging2 where industry like 'Crypto%' ;

-- lets update crypto to a single query 

update layoff_stagging2
set industry ='Crypto'
where industry like 'Crypto%';



select DISTINCT industry
from layoff_stagging2
order by 1;  # now that issue is covered 

select distinct country 
from layoff_stagging2 order by 1;


# noticed error in the country for US  as 'United States' and 'United States.'

select distinct country, trim(trailing'.' from country)
from layoff_stagging2
order by 1;

update layoff_stagging2
set country = trim(trailing'.' from country)
where country like 'United States%';

-- Change the date to datetime format 

select `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
from layoff_stagging2;

update layoff_stagging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

-- but this wont update the data definition in the original layoff_staging2 so will have to use ALTER

ALTER table layoff_stagging2
modify column `date` DATE;

select *
from layoff_stagging2 
where industry is null
or industry= '' ;

select * from layoff_stagging2
where company ='Airbnb';

update layoff_stagging2
set industry = NULL 
where industry='';

select *  from layoff_stagging2 t1
join layoff_stagging2 t2
on t1.company= t2.company
-- and t1.location =t2.location
where (t1.industry is NULL or t1.industry='') and t2.industry is NOT NULL;

update layoff_stagging2 t1
join layoff_stagging2 t2
 on t1.company =t2.company
 set t1.industry= t2.industry
 where(t1.industry is NULL OR t1.industry ='')
 and t2.industry is NOT NULL;

select distinct industry from layoff_stagging2;

select *
from layoff_stagging2 
where industry is null
or industry= '' ;

select * from layoff_stagging2
where company Like 'Bally%' ;

select * from layoff_stagging2
where total_laid_off is NULL and percentage_laid_off is NULL;

-- so there are multiple rows where the data is missing so what we can do is that we can delete this rows as we are not sure if there was a laid was even done or not 

Delete from layoff_stagging2
where total_laid_off is NULL and percentage_laid_off is NULL;

select * from layoff_stagging2;

Alter table layoff_stagging2
drop column row_num;

select * from layoff_stagging2;

















































































































