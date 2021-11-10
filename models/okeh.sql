
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
    "field": "updated_at",
    "data_type": "timestamp",
    "granularity": "day"
    }
)}}

SELECT 
    * 
FROM L1_creatory.cr_challenge_challenges_cuy
{% if is_incremental() %}
		
	-- recalculate latest day's data + previous
	where date(updated_at) >= max(updated_at)
		
    {% endif %}
    