SELECT * FROM world_layoffs.layoffs_staging;

-- add row_num column  that is partition over all columns to see the redundunt data 
select*,row_number()
over (partition by  company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte AS
(
select *,row_number()
over (partition by  company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging 
)
-- select all rows that have duplicates wich row_num mis more than 1
select * from duplicate_cte
where row_num >1;

select * from layoffs_staging 
where company="Casper";


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,row_number()
over (partition by  company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging ;

 select * from layoffs_staging2
where row_num >1;

DELETE from layoffs_staging2
WHERE row_num >1;

select * from layoffs_staging2
WHERE row_num >1;

select * from layoffs_staging2
;
-- standardization
select company,trim(company) 
from layoffs_staging2; 

select * from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select Distinct country ,trim(trailing '.'from country) 
from layoffs_staging2; 

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2 ;

Update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y') ;

select `date`
from layoffs_staging2 ;

Alter table layoffs_staging2
modify column `date` Date ;



select * from layoffs_staging2;

select distinct country,trim(trailing '.'from country) 
from layoffs_staging2
where country like 'united states %';

update layoffs_staging2
set country=trim(trailing '.'from country)
where country like 'United States%';
; 
select * from layoffs_staging2;

-- Null values
select * from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where (t1.industry is Null or t1.industry='');

-- this query we can see the null values from t1 and the join on t2 shows us the same companies with no null values in the indusrty field



-- change all blanks to nulls in order to populate them 
update layoffs_staging2
set industry= null
where industry='' ;

select * from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where (t1.industry is Null or t1.industry='') 
and t2.industry is not null;

 update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is Null 
and t2.industry is not null;

-- verify that indusrty field was populated from non null values to null values

select * from layoffs_staging2
where company='airbnb';

-- select the values that are nulls and need to be deleted
select * from layoffs_staging2

-- delete coloumn row_num
-- alter table layoffs_staging2
-- Drop column row_num
where total_laid_off is null
and percentage_laid_off is null ;

-- delete rows with null values
delete from  layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;


