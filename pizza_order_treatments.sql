
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


-- tabela fato de orders (1 order pode ter N order ids)
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


-- tabela fato de order details (1 order detail pode ter 1 order ids)
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


-- tabela de dimensão de pizzas
drop table if exists dim_pizzas_t6m;
create table dim_pizzas_t6m as 

    select    distinct 
              current_date as snapshot_date
            , pizza_id
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

    - fact_pizza_order_details_t12m     1.9    MB -> resultados dos últimos 6 meses, somente
    - fact_pizza_orders_t12m            1.4    MB -> resultados dos últimos 6 meses, somente
    - dim_pizzas_t12m                   0.024  MB -> resultados dos últimos 6 meses, somente

    Na prática, temos uma redução de 67% (total de 10MB para 3.3MB) no volume que deve ser processado durante uma análise, utilizando somente a janela de tempo necessária para a equipe.
*/







-- *****************************************************************************
-- Criação da tabela que será utilizada no Power BI para a análise dos dados
-- com métricas pré-calculadas de modo a evitar complexidade excessiva em DAX
-- *****************************************************************************

-- Imaginemos que a tarefa requisitada por um diretor de vendas seja dar visibilidade para (# = número):
-- 1. # de vendas mensais por categoria [Dificuldade: Fácil]
-- 2. # itens pedidos por mês por categoria (soma de quantidade) [Dificuldade: Fácil]
-- 3. # de pedidos distintos por itens (pizza_id) por mês [Dificuldade: Médio]
-- 4. Taxa de crescimento de vendas MoM [Dificuldade: Difícil]
-- 5. Taxa de crescimento de pedidos MoM [Dificuldade: Difícil]
-- 6. Taxa de crescimento de pedidos distintos por item MoM [Dificuldade: Difícil]

-- A tabela que será criada deve ser capaz de dar todos esses resultados de maneira simplificada para evitar cálculos complexos via DAX.
-- P.s.: É importante sempre termos uma data de snapshot para exibir para o usuário a última data de atualização

drop table if exists pbi_orders_analysis_t6m;
create table pbi_orders_analysis_t6m as 

    select *

    from 
    (
        -- Valores agregados a nível mensal, e à nível de produto
        select    current_timestamp as snapshot_date 
                
                -- PBI Filters by grain of the analysis
                , 'Mensal'  as date_period
                , 'Produto' as grain

                -- Dimensions

                , date_trunc('month', po.order_timestamp)::date      as order_period
                , dp.pizza_name || ' [ ID: ' || dp.pizza_id || ' ]'  as grain_infos
                , dp.pizza_name                                      as grain_name
                , dp.pizza_id                                        as grain_id
                , dp.pizza_category                                  as grain_category

                -- Calculations
                , count(distinct po.order_id) as num_dist_orders
                , sum(pod.total_price)        as sum_of_total_price
                , sum(pod.quantity)           as sum_of_quantity


                -- Metrics pre-calculations 
                , lag(count(distinct po.order_id), 1) over (partition by dp.pizza_id order by date_trunc('month', po.order_timestamp)::date asc) as previous_month_num_dist_orders
                , lag(sum(pod.total_price), 1) over (partition by dp.pizza_id order by date_trunc('month', po.order_timestamp)::date asc)        as previous_month_sum_total_price
                , lag(sum(pod.quantity), 1) over (partition by dp.pizza_id order by date_trunc('month', po.order_timestamp)::date asc)           as previous_month_sum_of_quantity


        from fact_pizza_orders_t6m po

            join fact_pizza_order_details_t6m pod
            on pod.order_details_id = po.order_details_id

            join dim_pizzas_t6m dp
            on dp.pizza_id = pod.pizza_id

        group by 1,2,3,4,5,6,7,8



        union all


        -- Valores agregados a nível mensal, e à nível de categoria
        select    current_timestamp as snapshot_date 
                
                -- PBI Filters by grain of the analysis
                , 'Mensal'    as date_period
                , 'Categoria' as grain

                -- Dimensions
                , date_trunc('month', po.order_timestamp)::date as order_period
                , '#'                                           as grain_infos
                , dp.pizza_category                             as grain_name
                , '#'                                           as grain_id
                , dp.pizza_category                             as grain_category                             

                -- Calculations
                , count(distinct po.order_id) as num_dist_orders
                , sum(pod.total_price)        as sum_of_total_price
                , sum(pod.quantity)           as sum_of_quantity


                -- Metrics pre-calculations 
                , lag(count(distinct po.order_id), 1) over (partition by dp.pizza_category order by date_trunc('month', po.order_timestamp)::date asc) as previous_month_num_dist_orders
                , lag(sum(pod.total_price), 1) over (partition by dp.pizza_category order by date_trunc('month', po.order_timestamp)::date asc)        as previous_month_sum_total_price
                , lag(sum(pod.quantity), 1) over (partition by dp.pizza_category order by date_trunc('month', po.order_timestamp)::date asc)           as previous_month_sum_of_quantity


        from fact_pizza_orders_t6m po

            join fact_pizza_order_details_t6m pod
            on pod.order_details_id = po.order_details_id

            join dim_pizzas_t6m dp
            on dp.pizza_id = pod.pizza_id

        group by 1,2,3,4,5,6,7,8


        union all


        -- Valores agregados a nível semanal, e à nível de produto
        select    current_timestamp as snapshot_date 
                
                -- PBI Filters by grain of the analysis
                , 'Semanal'  as date_period
                , 'Produto' as grain

                -- Dimensions

                , date_trunc('week', po.order_timestamp)::date       as order_period
                , dp.pizza_name || ' [ ID: ' || dp.pizza_id || ' ]'  as grain_infos
                , dp.pizza_name                                      as grain_name
                , dp.pizza_id                                        as grain_id
                , dp.pizza_category                                  as grain_category

                -- Calculations
                , count(distinct po.order_id) as num_dist_orders
                , sum(pod.total_price)        as sum_of_total_price
                , sum(pod.quantity)           as sum_of_quantity


                -- Metrics pre-calculations 
                , lag(count(distinct po.order_id), 1) over (partition by dp.pizza_id order by date_trunc('week', po.order_timestamp)::date asc) as previous_month_num_dist_orders
                , lag(sum(pod.total_price), 1) over (partition by dp.pizza_id order by date_trunc('week', po.order_timestamp)::date asc)        as previous_month_sum_total_price
                , lag(sum(pod.quantity), 1) over (partition by dp.pizza_id order by date_trunc('week', po.order_timestamp)::date asc)           as previous_month_sum_of_quantity


        from fact_pizza_orders_t6m po

            join fact_pizza_order_details_t6m pod
            on pod.order_details_id = po.order_details_id

            join dim_pizzas_t6m dp
            on dp.pizza_id = pod.pizza_id

        group by 1,2,3,4,5,6,7,8



        union all


        -- Valores agregados a nível semanal, e à nível de categoria
        select    current_timestamp as snapshot_date 
                
                -- PBI Filters by grain of the analysis
                , 'Semanal'    as date_period
                , 'Categoria' as grain

                -- Dimensions
                , date_trunc('week', po.order_timestamp)::date  as order_period
                , '#'                                           as grain_infos
                , dp.pizza_category                             as grain_name
                , '#'                                           as grain_id
                , dp.pizza_category                             as grain_category                             

                -- Calculations
                , count(distinct po.order_id) as num_dist_orders
                , sum(pod.total_price)        as sum_of_total_price
                , sum(pod.quantity)           as sum_of_quantity


                -- Metrics pre-calculations 
                , lag(count(distinct po.order_id), 1) over (partition by dp.pizza_category order by date_trunc('week', po.order_timestamp)::date asc) as previous_month_num_dist_orders
                , lag(sum(pod.total_price), 1) over (partition by dp.pizza_category order by date_trunc('week', po.order_timestamp)::date asc)        as previous_month_sum_total_price
                , lag(sum(pod.quantity), 1) over (partition by dp.pizza_category order by date_trunc('week', po.order_timestamp)::date asc)           as previous_month_sum_of_quantity


        from fact_pizza_orders_t6m po

            join fact_pizza_order_details_t6m pod
            on pod.order_details_id = po.order_details_id

            join dim_pizzas_t6m dp
            on dp.pizza_id = pod.pizza_id

        group by 1,2,3,4,5,6,7,8
    ) agg_orders

    order by grain_id asc, order_period desc
;



-- check results
select * from pbi_orders_analysis_t6m limit 5;