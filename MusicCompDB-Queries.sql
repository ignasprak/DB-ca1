set search_path="musiccompdb";

--Q1. 
SELECT 
    p.PARTNAME,
    e.EDYEAR,
    a.AGE_GROUP_DESC,
    vc.CATNAME,
    v.VOTEMODE,
    COUNT(v.VOTE) AS total_votes
FROM 
    VOTES v
JOIN 
    VIEWERS vw ON v.VIEWERID = vw.VIEWERID
JOIN 
    AGEGROUP a ON vw.AGE_GROUP = a.AGE_GROUPID
JOIN 
    VIEWERCATEGORY vc ON vw.CATEGORY = vc.CATID
JOIN 
    PARTICIPANTS p ON v.PARTNAME = p.PARTNAME
JOIN 
    Edition e ON v.EDITION_YEAR = e.EDYEAR
where v.edition_year in (2021, 2023)
GROUP BY 
    p.PARTNAME,
    e.EDYEAR,
    a.AGE_GROUP_DESC,
    vc.CATNAME,
    v.VOTEMODE
ORDER BY 
    p.PARTNAME,
    e.EDYEAR,
    a.AGE_GROUP_DESC,
    vc.CATNAME,
    v.VOTEMODE;


--Q2. 
SELECT 
    c.COUNTYNAME,
    p.PARTNAME,
    COUNT(v.VOTE) AS "Total Votes From Particpants County"
FROM 
    MusicCompDB.VOTES v
JOIN 
    MusicCompDB.PARTICIPANTS p ON v.PARTNAME = p.PARTNAME
JOIN 
    MusicCompDB.VIEWERS vw ON v.VIEWERID = vw.VIEWERID
JOIN 
    MusicCompDB.COUNTY c ON p.COUNTYID = c.COUNTYID AND vw.COUNTY = c.COUNTYID
WHERE 
    v.EDITION_YEAR in (2021, 2023)
    AND vw.CATEGORY = (SELECT CATID FROM MusicCompDB.VIEWERCATEGORY WHERE CATNAME = 'Audience')
GROUP BY 
    c.COUNTYNAME, 
    p.PARTNAME, 
    v.edition_year
ORDER BY 
    c.COUNTYNAME, 
    p.PARTNAME, 
    v.edition_year;

--Q3. 
SELECT 
    c.COUNTYNAME,
    v.EDITION_YEAR,
    v.VOTEMODE,
    SUM(
        CASE
            -- Charges for 2018
            WHEN v.EDITION_YEAR = 2021 AND v.VOTEMODE IN ('Facebook', 'Instagram') THEN 0.20
            WHEN v.EDITION_YEAR = 2021 AND v.VOTEMODE IN ('Web', 'Phone') THEN 0.50
            -- Charges for 2023
            WHEN v.EDITION_YEAR = 2023 AND v.VOTEMODE IN ('Facebook', 'Instagram') THEN 0.50
            WHEN v.EDITION_YEAR = 2023 AND v.VOTEMODE IN ('Web', 'Phone') THEN 1.00
            ELSE 0
        END
    ) AS TotalIncome
FROM 
    MusicCompDB.VOTES v
JOIN 
    MusicCompDB.VIEWERS vw ON v.VIEWERID = vw.VIEWERID
JOIN 
    MusicCompDB.COUNTY c ON vw.COUNTY = c.COUNTYID
WHERE 
    v.EDITION_YEAR IN (2021, 2023)
    AND vw.CATEGORY = (SELECT CATID FROM MusicCompDB.VIEWERCATEGORY WHERE CATNAME = 'Audience')
GROUP BY 
    c.COUNTYNAME, 
    v.EDITION_YEAR, 
    v.VOTEMODE
ORDER BY 
    c.COUNTYNAME, 
    v.EDITION_YEAR, 
    v.VOTEMODE;
