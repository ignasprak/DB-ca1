SET search_path = "musiccompdb";

-- Original Q1. 20-30ms cost
SELECT p.PARTNAME, e.EDYEAR, a.AGE_GROUP_DESC, vc.CATNAME, v.VOTEMODE, COUNT(v.VOTE) AS total_votes
FROM
    musiccompdb.VOTES v
    JOIN musiccompdb.VIEWERS vw ON v.VIEWERID = vw.VIEWERID
    JOIN musiccompdb.AGEGROUP a ON vw.AGE_GROUP = a.AGE_GROUPID
    JOIN musiccompdb.VIEWERCATEGORY vc ON vw.CATEGORY = vc.CATID
    JOIN musiccompdb.PARTICIPANTS p ON v.PARTNAME = p.PARTNAME
    JOIN musiccompdb.Edition e ON v.EDITION_YEAR = e.EDYEAR
WHERE
    v.edition_year IN (2021, 2023)
GROUP BY
    p.PARTNAME,
    e.EDYEAR,
    a.AGE_GROUP_DESC,
    vc.CATNAME,
    v.VOTEMODE
ORDER BY p.PARTNAME, e.EDYEAR, a.AGE_GROUP_DESC, vc.CATNAME, v.VOTEMODE;

-- Updated Q1 - less query code, on average 10-20ms cost

SELECT vf.EditionYear, pd.ParticipantName, cd.CountyName, vf.VoteMode, vf.ViewerAgeGrp, SUM(vf.TotalVotes) AS TotalVotes
FROM MusicCompDimDB_c20424992.VotesFact vf
    JOIN MusicCompDimDB_c20424992.ParticipantDimension pd ON vf.ParticipantName = pd.ParticipantName
    JOIN MusicCompDimDB_c20424992.CountyDimension cd ON pd.CountyID = cd.CountyID
WHERE
    vf.EditionYear IN (2021, 2023)
GROUP BY
    vf.EditionYear,
    pd.ParticipantName,
    cd.CountyName,
    vf.VoteMode,
    vf.ViewerAgeGrp
ORDER BY vf.EditionYear, pd.ParticipantName, SUM(vf.TotalVotes) DESC;

-- Original Q2. 10-20ms cost
SELECT
    c.COUNTYNAME,
    p.PARTNAME,
    COUNT(v.VOTE) AS "Total Votes From Particpants County"
FROM MusicCompDB.VOTES v
    JOIN MusicCompDB.PARTICIPANTS p ON v.PARTNAME = p.PARTNAME
    JOIN MusicCompDB.VIEWERS vw ON v.VIEWERID = vw.VIEWERID
    JOIN MusicCompDB.COUNTY c ON p.COUNTYID = c.COUNTYID
    AND vw.COUNTY = c.COUNTYID
WHERE
    v.EDITION_YEAR IN (2021, 2023)
    AND vw.CATEGORY = (
        SELECT CATID
        FROM MusicCompDB.VIEWERCATEGORY
        WHERE
            CATNAME = 'Audience'
    )
GROUP BY
    c.COUNTYNAME,
    p.PARTNAME,
    v.edition_year
ORDER BY c.COUNTYNAME, p.PARTNAME, v.edition_year;

-- Updated Q2 - less code, and on average 5-10ms cost
SELECT cd.CountyName, cf.ParticipantName, cf.TotalVotes
FROM MusicCompDimDB_c20424992.CountyVotesFact cf
    JOIN MusicCompDimDB_c20424992.CountyDimension cd ON cf.CountyID = cd.CountyID
WHERE
    cf.EditionYear IN (2021, 2023)
    AND cf.ViewerCategory = (
        SELECT CatID
        FROM MusicCompDimDB_c20424992.CategoryDimension
        WHERE
            CatName = 'Audience'
    )
ORDER BY cd.CountyName, cf.ParticipantName;

-- Original Q3. - averages the time cost at around 14-20ms
SELECT c.COUNTYNAME, v.EDITION_YEAR, v.VOTEMODE, SUM(
        CASE
        -- Charges for 2018
            WHEN v.EDITION_YEAR = 2021
            AND v.VOTEMODE IN ('Facebook', 'Instagram') THEN 0.20
            WHEN v.EDITION_YEAR = 2021
            AND v.VOTEMODE IN ('Web', 'Phone') THEN 0.50
            -- Charges for 2023
            WHEN v.EDITION_YEAR = 2023
            AND v.VOTEMODE IN ('Facebook', 'Instagram') THEN 0.50
            WHEN v.EDITION_YEAR = 2023
            AND v.VOTEMODE IN ('Web', 'Phone') THEN 1.00
            ELSE 0
        END
    ) AS TotalIncome
FROM MusicCompDB.VOTES v
    JOIN MusicCompDB.VIEWERS vw ON v.VIEWERID = vw.VIEWERID
    JOIN MusicCompDB.COUNTY c ON vw.COUNTY = c.COUNTYID
WHERE
    v.EDITION_YEAR IN (2021, 2023)
    AND vw.CATEGORY = (
        SELECT CATID
        FROM MusicCompDB.VIEWERCATEGORY
        WHERE
            CATNAME = 'Audience'
    )
GROUP BY
    c.COUNTYNAME,
    v.EDITION_YEAR,
    v.VOTEMODE
ORDER BY c.COUNTYNAME, v.EDITION_YEAR, v.VOTEMODE;

-- Updated Q3 - average time cost is 9-15ms
SELECT ed.EditionYear, cd.CountyName, vf.VoteMode, SUM(vf.VoteIncome * vf.TotalVotes) AS TotalIncome
FROM MusicCompDimDB_c20424992.VotesFact vf
    JOIN MusicCompDimDB_c20424992.CountyDimension cd ON vf.CountyID = cd.CountyID
    JOIN MusicCompDimDB_c20424992.EditionDimension ed ON vf.EditionYear = ed.EditionYear
WHERE
    ed.EditionYear IN (2021, 2023)
GROUP BY
    ed.EditionYear,
    cd.CountyName,
    vf.VoteMode
ORDER BY ed.EditionYear, cd.CountyName, vf.VoteMode;