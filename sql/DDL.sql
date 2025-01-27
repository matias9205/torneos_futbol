UPDATE [data_practise].[torneos_futbol].[matchs_data]
SET score = NULL
WHERE score IS NULL OR score NOT LIKE '%-%' OR score LIKE '---' or status IS NULL;

UPDATE [data_practise].[torneos_futbol].[matchs_data]
SET 
    local_goals = CASE 
        WHEN CHARINDEX('(', score) = 0 THEN LEFT(score, CHARINDEX('-', score) - 1) -- Formato "2-2"
        ELSE LEFT(score, CHARINDEX('(', score) - 1) -- Formato "2(2-4)1"
    END,
    away_goals = CASE
        WHEN CHARINDEX('(', score) = 0 THEN SUBSTRING(score, CHARINDEX('-', score) + 1, LEN(score) - CHARINDEX('-', score)) -- Formato "2-2"
        ELSE SUBSTRING(score, CHARINDEX(')', score) + 1, LEN(score) - CHARINDEX(')', score)) -- Formato "2(2-4)1"
    END
WHERE score IS NOT NULL AND score LIKE '%-%';

CREATE OR ALTER VIEW [torneos_futbol].[all_matchs] AS
SELECT 
	[tournament], --NOMBRE TORNEO--
	[country], --PAIS--
	[year], --A�O--
	[date], --FECHA--
	[home_team] AS local_team, --EQUIPO LOCAL--
	[away_team], --EQUIPO VISITANTE--
	CASE 
        WHEN CHARINDEX('(', score) > 0 THEN 
            LEFT(score, CHARINDEX('(', score) - 1) + '-' + 
            RIGHT(score, LEN(score) - CHARINDEX(')', score))
        ELSE score
    END AS score, --PUNTAJE FINAL
	CASE 
        WHEN CHARINDEX('(', score) > 0 THEN 
            SUBSTRING(score, CHARINDEX('(', score) + 1, CHARINDEX(')', score) - CHARINDEX('(', score) - 1)
        ELSE NULL
    END AS score_by_penalties, --PUNTAJE CON PENALES
	CAST([local_goals] AS INT) as [local_goals], --CANTIDAD DE GOLES EQUIPO LOCAL
	CAST([away_goals] AS INT) as [away_goals], --CANTIDAD DE GOLES EQUIPO VISITANTE
	(SELECT COUNT(*) FROM string_split(local_yellow_cards, ',') WHERE value <> '') AS local_yellow_cards, --CANTIDAD DE TARJETAS AMARILLAS EQUIPO LOCAL
	(SELECT COUNT(*) FROM string_split(away_yellow_cards, ',') WHERE value <> '') AS away_yellow_cards, --CANTIDAD DE TARJETAS AMARILLAS EQUIPO VISITANTE
	(SELECT COUNT(*) FROM string_split(local_red_cards, ',') WHERE value <> '') AS local_red_cards, --CANTIDAD DE TARJETAS ROJAS EQUIPO LOCAL
	(SELECT COUNT(*) FROM string_split(away_red_cards, ',') WHERE value <> '') AS away_red_cards, --CANTIDAD DE TARJETAS ROJAS EQUIPO VISITANTE
	CAST(REPLACE(local_ball_position, '%', '') AS INT) AS local_ball_position_numeric,
	CAST(REPLACE(away_ball_position, '%', '') AS INT) AS away_ball_position_numeric,
	CAST([local_kicks_to_goals] AS INT) AS [local_kicks_to_goals],
	CAST([away_kicks_to_goals] AS INT) AS [away_kicks_to_goals],
	CAST([local_outside_kicks] AS INT) AS [local_outside_kicks],
	CAST([away_outside_kicks] AS INT) AS [away_outside_kicks],
	CAST([local_total_kicks] AS INT) AS [local_total_kicks],
	CAST([away_total_kicks] AS INT) AS [away_total_kicks],
	CAST([local_shortcuts] AS INT) AS [local_shortcuts],
	CAST([away_shortcuts] AS INT) AS [away_shortcuts],
	CAST([local_corner_kicks] AS INT) AS [local_corner_kicks],
	CAST([away_corner_kicks] AS INT) AS [away_corner_kicks],
	CAST([local_offside] AS INT) AS [local_offside],
    CAST([away_offside] AS INT) AS [away_offside],
    CAST([local_lesions] AS INT) AS [local_lesions],
    CAST([away_lesions] AS INT) AS [away_lesions],
    CAST([local_faults] AS INT) AS [local_faults],
    CAST([away_faults] AS INT) AS [away_faults],
    CAST([local_crossbar_kicks] AS INT) AS [local_crossbar_kicks],
    CAST([away_crossbar_kicks] AS INT) AS [away_crossbar_kicks],
    CAST([local_commited_penalties] AS INT) AS [local_commited_penalties],
    CAST([away_commited_penalties] AS INT) AS [away_commited_penalties],
	(CASE 
        WHEN local_goals > away_goals THEN home_team
        WHEN away_goals > local_goals THEN away_team
        ELSE 'Draw'
    END) AS winner,
	(CASE 
        WHEN local_goals > away_goals THEN 3
        WHEN away_goals = local_goals THEN 1
        ELSE 0
    END) AS local_points,
	(CASE 
        WHEN local_goals < away_goals THEN 3
        WHEN away_goals = local_goals THEN 1
        ELSE 0
    END) AS away_points
FROM [torneos_futbol].[matchs_data]
WHERE status = 'FINALIZADO'

CREATE OR ALTER VIEW [torneos_futbol].[player_goals] AS
SELECT 
    t.tournament,
    t.year,
    t.team,
    t.player,
    COUNT(*) AS goals
FROM (
    -- Goles de jugadores locales
    SELECT 
        tournament,
        year,
        home_team AS team,
        value AS player
    FROM [torneos_futbol].[matchs_data]
    CROSS APPLY STRING_SPLIT(local_scorers, ',') 
    WHERE value <> ''
    
    UNION ALL
    
    -- Goles de jugadores visitantes
    SELECT 
        tournament,
        year,
        away_team AS team,
        value AS player
    FROM [torneos_futbol].[matchs_data]
    CROSS APPLY STRING_SPLIT(away_scorers, ',') 
    WHERE value <> ''
) t
GROUP BY t.tournament, t.year, t.team, t.player;