-- EDA Exploratory Data Analysis 

select * from layoff_stagging2;

select max(total_laid_off), min(total_laid_off)
from layoff_stagging2;

select max(percentage_laid_off) from layoff_stagging2;

select * from layoff_stagging2 where percentage_laid_off =1;

SELECT 
  company,
  ROW_NUMBER() OVER (ORDER BY `date`) AS running_total
FROM layoff_stagging2
WHERE percentage_laid_off = 1;

select * from layoff_stagging2
where percentage_laid_off=1
order by funds_raised_millions desc;

select company, sum(total_laid_off), sum(funds_raised_millions)
from layoff_stagging2
group by company order by 2 desc;

select industry, sum(total_laid_off), sum(funds_raised_millions)
from layoff_stagging2
group by industry order by 2 desc;

select year(`date`) year_, sum(total_laid_off)
from layoff_stagging2
group by year_ order by 1 desc;


select month(`date`) as Month_, substring(`date`,6,2) as Month_str
from layoff_stagging2;

select substring(`date`,1,7) as Month_str, sum(total_laid_off)
from layoff_stagging2 where substring(`date`,1,7) is not Null
group by Month_str
order by 1 asc;

with rolling_total as 
(select substring(`date`,1,7) as Month_str, sum(total_laid_off) as total_off
from layoff_stagging2 where substring(`date`,1,7) is not Null
group by Month_str
order by 1 asc
)
select Month_str,total_off, sum(total_off) over( order by Month_str) as rolling_total
from rolling_total;

select company, year(`date`) year_, sum(total_laid_off)
from layoff_stagging2
group by company, year_
order by 1 asc;

with company_year (company, years, total_laid_off) as 
(select company, year(`date`) year_, sum(total_laid_off)
from layoff_stagging2
group by company, year_),
company_year_rank as (select *, 
dense_rank() over (Partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null)
select * from company_year_rank 
where ranking <=5;

select * from layoff_stagging2;
SELECT 
  country,
  company,
  substring(`date`,1,7) as year_,
  SUM(total_laid_off)
FROM layoff_stagging2
GROUP BY country, company, year_
ORDER BY country;

with cte_ (country, company, years, total_laid_off) as 
(SELECT 
  country,
  company,
  substring(`date`,1,7) as year_,
  SUM(total_laid_off)
FROM layoff_stagging2
GROUP BY country, company, year_)
select country, company, total_laid_off, years,
dense_rank() over (Partition by years order by total_laid_off desc) as ranking
from cte_
where years is not null
order by total_laid_off;


with cte_ (country, company, years, total_laid_off) as 
(SELECT 
  country,
  company,
  substring(`date`,1,7) as year_,
  SUM(total_laid_off)
FROM layoff_stagging2
GROUP BY country, company, year_),
company_ranking as(select country, company, total_laid_off, years,
dense_rank() over (Partition by years order by total_laid_off  desc) as ranking
from cte_
where years is not null)
select * from company_ranking
where ranking <=5;



WITH cte_ (country, company, years, total_laid_off) AS (
  SELECT 
    country,
    company,
    substring(`date`,1,7) as year_,
    SUM(total_laid_off)
  FROM layoff_stagging2
  GROUP BY country, company, year_
),
company_ranking AS (
  SELECT 
    country, 
    company, 
    total_laid_off, 
    years,
    DENSE_RANK() OVER (
      PARTITION BY country, years 
      ORDER BY total_laid_off DESC
    ) AS ranking
  FROM cte_
  WHERE years IS NOT NULL
)
SELECT *
FROM company_ranking
WHERE ranking <= 5
ORDER BY country, years, ranking;

WITH cte_agg AS (
  SELECT 
    country,
    company,
    substring(`date`,1,7) as year_,
    SUM(total_laid_off) AS total_laid_off
  FROM layoff_stagging2
  WHERE percentage_laid_off IS NOT NULL
  GROUP BY country, company, year_
),
cte_ranked AS (
  SELECT 
    country,
    company,
    year_,
    total_laid_off,
    DENSE_RANK() OVER (
      PARTITION BY country 
      ORDER BY total_laid_off DESC
    ) AS ranking
  FROM cte_agg
)
SELECT *
FROM cte_ranked
ORDER BY country, ranking, year_;







































