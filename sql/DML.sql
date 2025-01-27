USE data_practise

SELECT DISTINCT tournament, country, year
FROM [torneos_futbol].[matchs_data]
ORDER BY year DESC

SELECT * FROM [torneos_futbol].[matchs_data]
--WHERE year = 2024
WHERE score LIKE '%(%'

SELECT * FROM [torneos_futbol].[matchs_data]
--WHERE tournament = 'copa sudamericana'
WHERE status = 'FINALIZADO'
ORDER BY year DESC

SELECT * FROM [torneos_futbol].[all_matchs]
WHERE year = 2024
AND
tournament IN ('primera division argentina')

SELECT TOP 10 * FROM [torneos_futbol].[all_matchs] ORDER BY NEWID()

SELECT [winner], COUNT([winner]) AS matchs_wins, year
FROM [torneos_futbol].[all_matchs]
WHERE [winner] NOT LIKE '%Draw%' AND [country] = 'Argentina'
GROUP BY [winner], [year]
ORDER BY matchs_wins DESC

SELECT 
    team,
	tournament,
    year,
    SUM(points) AS total_points
FROM (
    SELECT 
        local_team AS team,
		tournament,
        year,
        local_points AS points
    FROM [torneos_futbol].[all_matchs]
    WHERE [country] = 'Argentina'
    UNION ALL
    SELECT 
        away_team AS team,
		tournament,
        year,
        away_points AS points
    FROM [torneos_futbol].[all_matchs]
    WHERE [country] = 'Argentina'
) AS combined
WHERE year = 2024 AND tournament IN ('primera division argentina')
GROUP BY team, tournament, year
ORDER BY total_points DESC;

SELECT team,
	tournament,
    year,
	COUNT(*) as PJ,
	SUM(CASE WHEN team = winner THEN 1 ELSE 0 END) AS G,
	SUM(CASE WHEN winner = 'Draw' THEN 1 ELSE 0 END) AS E,
	SUM(CASE WHEN team <> winner AND winner <> 'Draw' THEN 1 ELSE 0 END) AS P,
	SUM(points) as PTS
FROM 
(SELECT 
    local_team AS team,
	tournament,
    year,
    local_points AS points,
	--local_goals AS goals,
	winner
FROM [torneos_futbol].[all_matchs]
UNION ALL
SELECT 
    away_team AS team,
	tournament,
    year,
    away_points AS points,
	--away_goals AS goals,
	winner
FROM [torneos_futbol].[all_matchs]) 
as combined
WHERE year = 2024 AND tournament IN ('primera')
GROUP BY team, tournament, year
ORDER BY PTS DESC;