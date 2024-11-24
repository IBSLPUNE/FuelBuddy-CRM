import frappe
from frappe.utils import today

def execute(filters=None):
    query = """
    WITH RECURSIVE dates_in_week AS (
        SELECT DATE(DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY)) AS day
        UNION ALL
        SELECT DATE_ADD(day, INTERVAL 1 DAY)
        FROM dates_in_week
        WHERE day < DATE(DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY)) + INTERVAL 6 DAY
    ),
    working_days_in_week AS (
        SELECT day, DAYOFWEEK(day) AS day_of_week
        FROM dates_in_week
    ),
    work_days_summary_week AS (
        SELECT 
            COUNT(*) AS working_days_week,
            SUM(CASE WHEN day <= CURRENT_DATE() THEN 1 ELSE 0 END) AS days_passed_week
        FROM working_days_in_week
        WHERE day_of_week NOT IN (1)
    ),
    dates_in_month AS (
        SELECT DATE(DATE_FORMAT(CURRENT_DATE(), '%%Y-%%m-01')) AS day
        UNION ALL
        SELECT DATE_ADD(day, INTERVAL 1 DAY)
        FROM dates_in_month
        WHERE day < LAST_DAY(CURRENT_DATE())
    ),
    working_days_in_month AS (
        SELECT day, DAYOFWEEK(day) AS day_of_week
        FROM dates_in_month
    ),
    work_days_summary_month AS (
        SELECT 
            COUNT(*) AS working_days_month
        FROM working_days_in_month
        WHERE day_of_week NOT IN (1)
    ),
    current_day_of_month AS (
        SELECT DAY(CURRENT_DATE()) AS day_of_month
    ),
    daily_achivement AS (
        SELECT bd, sales_target,
               SUM(CASE WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN daily_achievement ELSE 0 END) AS yesterday_achivement,
               SUM(CASE WHEN date = CURRENT_DATE() THEN daily_achievement ELSE 0 END) AS today_achivement
        FROM `tabDaily Achievement`
        GROUP BY 1, 2
    ),
    weekly_achivement AS (
        SELECT bd, sales_target,
               SUM(CASE 
                       WHEN WEEK(date) = WEEK(CURRENT_DATE()) 
                            AND YEAR(date) = YEAR(CURRENT_DATE())
                       THEN weekly_achievement 
                       ELSE 0 
                   END) AS week_till_date_achievement
        FROM `tabWeekly Achievement`
        GROUP BY 1, 2
    ),
    monthly_achivement AS (
        SELECT bd, sales_target,
               SUM(CASE 
                       WHEN MONTH(date) = MONTH(CURRENT_DATE()) 
                            AND YEAR(date) = YEAR(CURRENT_DATE())
                       THEN monthly_achievement
                       ELSE 0 
                   END) AS month_till_date_achievement
        FROM `tabMonthly Achievement`
        GROUP BY 1, 2
    ),
    zonal_target AS (
        SELECT parent, zonal_head,
               SUM(zones_target) AS zones_target
        FROM `tabZonal`
        GROUP BY 1, 2
    ),
    customer_count AS (
        SELECT account_manager,
               COUNT(DISTINCT name) AS customer_count
        FROM `tabCustomer`
        GROUP BY 1
    ),
    sales_person AS (
        SELECT name AS team_user_name,
               custom_user, custom_team
        FROM `tabSales Person`
    ),
    ranked_data AS (
        SELECT 
            business_head,
            posting_date,
            name,
            sales_person,
            ROW_NUMBER() OVER (PARTITION BY business_head ORDER BY posting_date DESC) AS rank
        FROM 
            `tabSales Target`
    )
    SELECT DISTINCT
        st.business_head AS bushead,
        st.sales_person AS slp,
        sp.custom_team AS Team,
        bd.zonal_head as Head,
        bd.zonal_head_sales_person as Head_Name,
        sp.team_user_name AS Team_Username,
        bd.bd_sales_person AS BD_name,
        bd.bd,
        cc.customer_count AS Customer_Count,
        ROUND(bd.daily_targerts , 2) AS LWD_TGT,
        ROUND(da.yesterday_achivement) AS LWD_Actual,
        ROUND(CASE 
            WHEN COALESCE(ROUND(bd.daily_targerts / 100000, 2), 0) = 0 THEN 0 
            ELSE (COALESCE(ROUND(da.yesterday_achivement / 100000, 2), 0) / COALESCE(ROUND(bd.daily_targerts / 100000, 2), 0)) * 100 
        END, 0) AS LWD_ATT,
        ROUND(bd.weekly_targets / 100000, 2) AS Week_TGT,
        ROUND((bd.weekly_targets / (100000 * (SELECT working_days_week FROM work_days_summary_week))) * (SELECT days_passed_week FROM work_days_summary_week), 2) AS WTD_TGT,
        ROUND(wa.week_till_date_achievement / 100000, 2) AS WEEK_Actual,
        ROUND(CASE 
            WHEN COALESCE(ROUND(bd.weekly_targets / 100000, 2), 0) = 0 THEN 0 
            ELSE (COALESCE(ROUND(wa.week_till_date_achievement / 100000, 2), 0) / COALESCE(ROUND(bd.weekly_targets / 100000, 2), 0)) * 100 
        END, 0) AS WEEK_ATT,
        ROUND(bd.monthly_targets / 100000, 2) AS Month_TGT,
        ROUND(bd.monthly_targets / (100000 * (SELECT working_days_month FROM work_days_summary_month)) * (SELECT day_of_month FROM current_day_of_month), 2) AS MTD_TGT,
        ROUND(ma.month_till_date_achievement / 100000, 2) AS MTD_Actual,
        ROUND((bd.monthly_targets / (100000 * (SELECT working_days_month FROM work_days_summary_month)) * (SELECT day_of_month FROM current_day_of_month)) - (ma.month_till_date_achievement / 100000), 2) AS MTD_Short,
        CONCAT(
            ROUND(CASE 
                WHEN COALESCE(ROUND(bd.monthly_targets / 100000, 2), 0) = 0 THEN 0 
                ELSE (COALESCE(ROUND(ma.month_till_date_achievement / 100000, 2), 0) / COALESCE(ROUND(bd.monthly_targets / 100000, 2), 0)) * 100 
            END, 0), '-'
        ) AS `MTD_ATT`,
        ROUND(bd.quarterly_targets / 100000, 2) AS Quaterly_TGT
    FROM ranked_data st
    LEFT JOIN `tabBusiness Deverloper` bd ON bd.parent = st.name
    LEFT JOIN `daily_achivement` da ON da.bd = bd.bd AND da.sales_target = st.name 
    LEFT JOIN `weekly_achivement` wa ON wa.bd = bd.bd AND wa.sales_target = st.name
    LEFT JOIN `monthly_achivement` ma ON ma.bd = bd.bd AND ma.sales_target = st.name
    LEFT JOIN sales_person sp ON sp.custom_user = bd.bd
    LEFT JOIN zonal_target tz ON tz.parent = st.name AND tz.zonal_head = bd.zonal_head
    LEFT JOIN customer_count cc ON cc.account_manager = bd.bd
    WHERE rank = 1
      AND (
        (bd.zonal_head = %s ) -- Fetch data for the logged-in zonal_head
        OR (bd.bd = %s)                      -- Fetch data for the logged-in BD
        OR (st.business_head = %s)     -- Fetch data if the logged-in user is the BD
      ) GROUP BY bd.bd
    """
    data = frappe.db.sql(query, (frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)
    columns = [
        {"fieldname": "Team", "label": "Team", "fieldtype": "Data"},
        {"fieldname": "slp", "label": "Sales Person", "fieldtype": "Data"},
        {"fieldname": "bushead", "label": "Bussiness Head", "fieldtype": "Data"},
        {"fieldname": "Head", "label": "Zonal Head", "fieldtype": "Data"},
        {"fieldname": "Head_Name", "label": "Zonal Head Name", "fieldtype": "Data"},
        {"fieldname": "Team_Username", "label": "Team Username", "fieldtype": "Data"},
        {"fieldname": "BD_name", "label": "BD Name", "fieldtype": "Data"},
        {"fieldname": "bd", "label": "BD", "fieldtype": "Data"},
        {"fieldname": "Customer_Count", "label": "Customer Count", "fieldtype": "Int"},
        {"fieldname": "LWD_TGT", "label": "LWD Target", "fieldtype": "Float"},
        {"fieldname": "LWD_Actual", "label": "LWD Actual", "fieldtype": "Float"},
        {"fieldname": "LWD_ATT", "label": "LWD ATT", "fieldtype": "Percent"},
        {"fieldname": "Week_TGT", "label": "Week Target", "fieldtype": "Float"},
        {"fieldname": "WTD_TGT", "label": "WTD Target", "fieldtype": "Float"},
        {"fieldname": "WEEK_Actual", "label": "Week Actual", "fieldtype": "Float"},
        {"fieldname": "WEEK_ATT", "label": "Week ATT", "fieldtype": "Float"},
        {"fieldname": "Month_TGT", "label": "Month Target", "fieldtype": "Float"},
        {"fieldname": "MTD_TGT", "label": "MTD Target", "fieldtype": "Float"},
    ]
    return columns, data

