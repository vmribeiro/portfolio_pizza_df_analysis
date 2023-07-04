
-- *********************************************************
-- Verificação dos dados inseridos utilizando pandas
-- ********************************************************

    -- verificar se os dados foram inseridos
    select * 

    from raw_pizza_orders 

    limit 10;

    -- verificar formato dos dados
    select    table_name
            , column_name 
            , data_type 

    from information_schema.columns

    where table_name = 'raw_pizza_orders';





-- ****************************************************************
-- Criação da tabela core com os dados "crus" (raw) pré-tratados
-- ***************************************************************

-- excluir a tabela caso já exista
drop table if exists core_pizza_orders;

-- criar a tabela
create table core_pizza_orders as
    
    select    order_details_id	
            , order_id	
            , pizza_id	
            , quantity	
            , to_timestamp(order_date || ' ' || order_time, 'DD/MM/YYYY HH24:MI:SS') as order_timestamp
            , unit_price	
            , total_price	
            , pizza_size	
            , pizza_category	
            , pizza_ingredients	
            , pizza_name
    
    from raw_pizza_orders;


-- select * from core_pizza_orders limit 5;




-- ****************************************************************
-- Criação do modelo star/snowflake
--
-- * nesse caso, optei pelo star dado o baixo volume de informações repetidas (ex.: em categorias)
-- ***************************************************************


-- criar tabela fato de orders (1 order pode ter N order ids)
-- t12m significa "trailing 12 months": últimos 12 meses a partir da última data existente na tabela. 
-- A estratégia de últimos meses é muito utilizado para reduzir volume em tabelas de data warehouses.

drop table if exists fact_pizza_orders_t6m;
create table fact_pizza_orders_t6m as
    
    select    distinct 
              current_date as snapshot_date
            , order_timestamp
            , order_id	
            , order_details_id	
    
    from core_pizza_orders
    
    where order_timestamp >=  (select MAX(order_timestamp) from core_pizza_orders) - interval '6 months' 

;


drop table if exists fact_pizza_order_details_t6m;
create table fact_pizza_order_details_t6m as 

    select    distinct 
              current_date as snapshot_date
            , order_details_id
            , pizza_id	
            , quantity	
            , unit_price
            , total_price

    from core_pizza_orders
    
    where order_timestamp >=  (select MAX(order_timestamp) from core_pizza_orders) - interval '6 months' 
;


drop table if exists dim_pizzas_t6m;
create table dim_pizzas_t6m as 

    select    distinct 
              current_date as snapshot_date
            , pizza_size	
            , pizza_category	
            , pizza_ingredients	
            , pizza_name
    
    from core_pizza_orders
;



-- validar valores em bytes das tabelas criadas para verificar a redução de volume

select    relname as relation
        , pg_size_pretty ( pg_total_relation_size (c .oid) ) as total_size

from pg_class c

    left join pg_namespace n 
    on (n.oid = c .relnamespace)

where nspname not in ( 'pg_catalog', 'information_schema' )
    and c .relkind <> 'i'
    and nspname !~ '^pg_toast'

order by pg_total_relation_size (c .oid) desc ;

/*
    Resultados:

    - raw_pizza_orders	                10     MB -> resultados dos últimos 12 meses
    - core_pizza_orders	                9.6    MB -> resultados dos últimos 12 meses pré-tratados

    - fact_pizza_order_details_t12m	    1.9    MB -> resultados dos últimos 6 meses, somente
    - fact_pizza_orders_t12m	        1.4    MB -> resultados dos últimos 6 meses, somente
    - dim_pizzas_t12m	                0.024  MB -> resultados dos últimos 6 meses, somente

    Na prática, temos uma redução de 67% (total de 10MB para 3.3MB) no volume que deve ser processado durante uma análise, utilizando somente a janela de tempo necessária para a equipe.
*/




-- *****************************************************************************
-- Criação da tabela que será utilizada no power bi para a análise dos dados
-- com métricas pré-calculadas de modo a evitar complexidade excessiva em DAX
-- *****************************************************************************

-- Imaginemos que a tarefa requisitada por um diretor de vendas seja dar visibilidade para (# = número):
-- 1. # de vendas mensais [Dificuldade: Fácil]
-- 2. # itens pedidos [Dificuldade: Fácil]
-- 3. # de pedidos distintos por itens [Dificuldade: Médio]
-- 4. Taxa de crescimento de vendas MoM [Dificuldade: Difícil]
-- 5. Taxa de crescimento de pedidos MoM [Dificuldade: Difícil]
-- 6. Taxa de crescimento de pedidos distintos por item MoM [Dificuldade: Difícil]