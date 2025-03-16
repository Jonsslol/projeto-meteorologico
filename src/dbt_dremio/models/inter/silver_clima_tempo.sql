{{
    config(
        materialized='table',
        alias='silver_clima'
    )
}}

SELECT
    CAST("data_medicao" AS DATE) AS dara_de_medicao,
    
    -- Colunas numéricas com tratamento de strings vazias
    CASE 
        WHEN "precipitacao_total_diario_autmm" = '' THEN NULL 
        ELSE CAST("precipitacao_total_diario_autmm" AS FLOAT) 
    END AS "PRECIPITACAO TOTAL, DIARIO (AUT)(mm)",
    
    CASE 
        WHEN "pressao_atmosferica_media_diaria_autmb" = '' THEN NULL 
        ELSE CAST("pressao_atmosferica_media_diaria_autmb" AS DECIMAL(8,2)) 
    END AS "PRESSAO ATMOSFERICA MEDIA DIARIA (AUT)(mB)",
    
    -- Padronização das demais colunas seguindo o mesmo padrão
    CASE 
        WHEN "temperatura_do_ponto_de_orvalho_media_diaria_autc" = '' THEN NULL 
        ELSE CAST("temperatura_do_ponto_de_orvalho_media_diaria_autc" AS DECIMAL(5,2)) 
    END AS "TEMPERATURA DO PONTO DE ORVALHO MEDIA DIARIA (AUT)(°C)",
    
    CASE 
        WHEN "temperatura_maxima_diaria_autc" = '' THEN NULL 
        ELSE CAST("temperatura_maxima_diaria_autc" AS DECIMAL(5,2)) 
    END AS "TEMPERATURA MAXIMA, DIARIA (AUT)(°C)",
    
    CASE 
        WHEN "temperatura_media_diaria_autc" = '' THEN NULL 
        ELSE CAST("temperatura_media_diaria_autc" AS DECIMAL(5,2)) 
    END AS "TEMPERATURA MEDIA, DIARIA (AUT)(°C)",
    
    CASE 
        WHEN "temperatura_minima_diaria_autc" = '' THEN NULL 
        ELSE CAST("temperatura_minima_diaria_autc" AS DECIMAL(5,2)) 
    END AS "TEMPERATURA MINIMA, DIARIA (AUT)(°C)",
    
    CASE 
        WHEN "umidade_relativa_do_ar_media_diaria_aut%" = '' THEN NULL 
        ELSE CAST("umidade_relativa_do_ar_media_diaria_aut%" AS DECIMAL(5,2)) 
    END AS "UMIDADE RELATIVA DO AR, MEDIA DIARIA (AUT)(%)",
    
    CASE 
        WHEN "umidade_relativa_do_ar_minima_diaria_aut%" = '' THEN NULL 
        ELSE CAST("umidade_relativa_do_ar_minima_diaria_aut%" AS DECIMAL(5,2)) 
    END AS "UMIDADE RELATIVA DO AR, MINIMA DIARIA (AUT)(%)",
    
    CASE 
        WHEN "vento_rajada_maxima_diaria_autm/s" = '' THEN NULL 
        ELSE CAST("vento_rajada_maxima_diaria_autm/s" AS DECIMAL(6,2)) 
    END AS "VENTO, RAJADA MAXIMA DIARIA (AUT)(m/s)",
    
    CASE 
        WHEN "vento_velocidade_media_diaria_autm/s" = '' THEN NULL 
        ELSE CAST("vento_velocidade_media_diaria_autm/s" AS DECIMAL(6,2)) 
    END AS "VENTO, VELOCIDADE MEDIA DIARIA (AUT)(m/s)",
    
    NULLIF("codigo_estacao", '') AS codigo_estacao

FROM {{ source('raw_clima_tempo', 'dados') }}