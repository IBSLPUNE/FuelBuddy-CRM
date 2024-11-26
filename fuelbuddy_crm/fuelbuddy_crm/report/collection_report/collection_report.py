import frappe
from frappe.utils import flt, round_based_on_smallest_currency_fraction

def execute(filters=None):
    columns = get_columns()
    data = get_data(filters)
    return columns, data

def get_columns():
    return [
        {"label": "Business Head", "fieldname": "bushead", "fieldtype": "Data", "width": 150},
        {"label": "Sales Person", "fieldname": "slp", "fieldtype": "Data", "width": 150},
        {"label": "Team", "fieldname": "team", "fieldtype": "Data", "width": 150},
        {"label": "Zonal Head", "fieldname": "head", "fieldtype": "Data", "width": 150},
        {"label": "Zonal Head Name", "fieldname": "head_name", "fieldtype": "Data", "width": 150},
        {"label": "Team Username", "fieldname": "team_username", "fieldtype": "Data", "width": 150},
        {"label": "BD Name", "fieldname": "bd_name", "fieldtype": "Data", "width": 150},
        {"label": "Net Outstanding Amount", "fieldname": "net_outstanding_amount", "fieldtype": "Currency", "width": 150},
        {"label": "Net Overdue Amount", "fieldname": "net_overdue_amount", "fieldtype": "Currency", "width": 150},
        {"label": "Last Working Day Target", "fieldname": "lwd_tgt", "fieldtype": "Float", "width": 150},
        {"label": "Last Working Day Actual", "fieldname": "lwd_actual", "fieldtype": "Float", "width": 150},
        {"label": "Last Working Day Attainment (%)", "fieldname": "lwd_att", "fieldtype": "Percent", "width": 150},
        {"label": "Weekly Target", "fieldname": "week_tgt", "fieldtype": "Float", "width": 150},
        {"label": "Weekly Collection", "fieldname": "week_collection", "fieldtype": "Float", "width": 150},
        {"label": "Weekly Attainment (%)", "fieldname": "week_att", "fieldtype": "Percent", "width": 150},
        {"label": "Monthly Target", "fieldname": "month_tgt", "fieldtype": "Float", "width": 150},
        {"label": "Monthly Collection", "fieldname": "month_collection", "fieldtype": "Float", "width": 150},
        {"label": "Monthly Attainment (%)", "fieldname": "month_att", "fieldtype": "Percent", "width": 150},
    ]

def get_data(filters):
    query = """
    WITH RECURSIVE dates_in_week AS (
        SELECT 
            DATE(DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY)) AS day -- Start of the current week (Monday)
        UNION ALL
        SELECT 
            DATE_ADD(day, INTERVAL 1 DAY)
        FROM dates_in_week
        WHERE day < DATE(DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY)) + INTERVAL 6 DAY -- End of the current week (Sunday)
    ),
    working_days_in_week AS (
        SELECT 
            day,
            DAYOFWEEK(day) AS day_of_week
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
        SELECT 
            DATE(DATE_FORMAT(CURRENT_DATE(), '%%Y-%%m-01')) AS day -- Start of the current month
        UNION ALL
        SELECT 
            DATE_ADD(day, INTERVAL 1 DAY)
        FROM dates_in_month
        WHERE day < LAST_DAY(CURRENT_DATE()) -- End of the current month
    ),
    working_days_in_month AS (
        SELECT 
            day,
            DAYOFWEEK(day) AS day_of_week
        FROM dates_in_month
    ),
    work_days_summary_month AS (
        SELECT 
            COUNT(*) AS working_days_month
        FROM working_days_in_month
        WHERE day_of_week NOT IN (1) -- Exclude Sundays
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
                    WHEN EXTRACT(WEEK FROM date) = EXTRACT(WEEK FROM CURRENT_DATE()) 
                            AND EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE())
                    THEN weekly_achievement 
                    ELSE 0 
                END) AS week_till_date_achievement
        FROM `tabWeekly Achievement`
        GROUP BY 1, 2
    ),
    monthly_achivement AS (
        SELECT bd, sales_target,
            SUM(CASE 
                    WHEN EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE()) 
                            AND EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE())
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
    ),
    customer AS (
        SELECT name,
            CASE WHEN custom_team IS NULL THEN 'Others' ELSE custom_team END AS custom_team,
            account_manager
        FROM `tabCustomer`
    ),
    yesterday_collection AS (
        SELECT
            tc.account_manager,
            SUM(CASE WHEN DATE(tPe.creation) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN paid_amount ELSE 0 END) AS amount
        FROM `tabPayment Entry` tPe
        LEFT JOIN `tabCustomer` tc ON tc.name = tPe.party
        WHERE tPe.party IS NOT NULL 
        AND status = 'Submitted'
        AND tPe.docstatus = 1
        GROUP BY 1
    ),
    week_collection AS (
        SELECT
            tc.account_manager,
            SUM(CASE 
                    WHEN EXTRACT(WEEK FROM DATE(tPe.creation)) = EXTRACT(WEEK FROM CURRENT_DATE())
                        AND EXTRACT(YEAR FROM DATE(tPe.creation)) = EXTRACT(YEAR FROM CURRENT_DATE())
                    THEN paid_amount 
                    ELSE 0 
                END) AS week_collection
        FROM `tabPayment Entry` tPe
        LEFT JOIN `tabCustomer` tc ON tc.name = tPe.party
        WHERE tPe.party IS NOT NULL 
        AND status = 'Submitted'
        AND tPe.docstatus = 1
        GROUP BY 1
    ),
    month_collection AS (
        SELECT
            tc.account_manager,
            SUM(CASE 
                    WHEN EXTRACT(MONTH FROM DATE(tPe.creation)) = EXTRACT(MONTH FROM CURRENT_DATE())
                        AND EXTRACT(YEAR FROM DATE(tPe.creation)) = EXTRACT(YEAR FROM CURRENT_DATE())
                    THEN paid_amount 
                    ELSE 0 
                END) AS month_collection
        FROM `tabPayment Entry` tPe
        LEFT JOIN `tabCustomer` tc ON tc.name = tPe.party
        WHERE tPe.party IS NOT NULL 
        AND status = 'Submitted'
        AND tPe.docstatus = 1
        GROUP BY 1
    ),
    latest_si_by_sales_order AS (
        SELECT tc.account_manager,
            SUM(si.outstanding_amount) AS outstanding_amount
        FROM `tabSales Invoice` si
        LEFT JOIN `tabCustomer` tc ON tc.name = si.customer
        GROUP BY 1
    ),
		gl_entry_total as (
			SELECT 
			party,
		tc.account_manager,
			SUM(debit - credit) AS total_gl_amount
		FROM 
			`tabGL Entry` AS glEntry
			left join `tabCustomer` tc on tc.name = party
		WHERE 
			glEntry.creation >= "2024-04-01" 
			AND glEntry.against IS NOT NULL 
			AND glEntry.party IS NOT NULL 
			AND glEntry.party_type  = "Customer"
			AND glEntry.party_type IS NOT NULL
			AND glEntry.name NOT IN(
		SELECT 
			glEntry.name
		FROM 
			`tabGL Entry` AS glEntry
		LEFT JOIN 
			`tabJournal Entry` AS tJe ON tJe.name = glEntry.voucher_no
		WHERE 
			glEntry.creation >= "2024-04-01" 
			AND tJe.docstatus != 1)
		GROUP BY 1,2
		), 
		jv_total  as (
			SELECT
			party,
			tc.account_manager,
			SUM(credit - debit) AS total_jv_amount
		FROM `tabJournal Entry` tJe
		LEFT JOIN `tabJournal Entry Account` tJea ON tJea.parent = tJe.name
		left join `tabCustomer` tc on tc.name = tJea.party
		where 
			tJe.voucher_type IN ('Bank Entry',
		'Cash Entry',
		'Credit Card Entry',
		'Debit Note',
		'Credit Note',
		'Contra Entry',
		'Excise Entry',
		'Write Off Entry',
		'Opening Entry',
		'Depreciation Entry',
		'Exchange Rate Revaluation',
		'Exchange Gain Or Loss',
		'Deferred Revenue',
		'Deferred Expense',
		'Reversal Of ITC',
		'Inter Company Journal Entry',
		'Journal Entry')
			AND tJea.party_type = 'Customer'
			AND tJea.party IS NOT NULL
			AND tJe.docstatus = 1
			AND tJea.docstatus = 1
			AND tJea.reference_type is NULL
		GROUP BY 1,2
		),
		payment_entry_total as (
			SELECT party,
			tc.account_manager,
			SUM(tPe.unallocated_amount) AS total_unallocated_amount
		FROM
			`tabPayment Entry` tPe
			left join `tabCustomer` tc on tc.name = tPe.party
		WHERE
			tPe.party IS NOT NULL AND
			tPe.docstatus =1
		GROUP BY 1,2
		),
		latest_si_by_sales_order_overdue as (
		SELECT 
			si.sales_order, 
			si.name, 
			si.posting_date, 
			si.posting_time, 
			si.grand_total, 
			si.outstanding_amount, 
			si.total_advance, 
			si.set_warehouse, 
			si.status, 
			si.customer, 
			si.contact_person, 
			si.shipping_address_name, 
			si.docstatus,
			case when si.status not in ('Cancelled','Paid') then si.posting_date else null end as posting_date2,
			ROW_NUMBER() OVER (
			PARTITION BY si.sales_order 
			ORDER BY 
				si.posting_date DESC, 
				si.posting_time DESC
			) AS rn 
		FROM 
			`tabSales Invoice` si
			),
			party_not_include_customer AS (   
		SELECT distinct parent as customer_code FROM `tabParty Account`
		WHERE 
			account IN("Trade Receivable for Inactive - TSL","Trade Receiavable for Franchisee - TSL")
			),
			overdue_amount_group as (
			select
			tc.name as customer_id,
			tc.account_manager,
			CAST(REGEXP_REPLACE(tc.payment_terms, '[^0-9]', '') AS UNSIGNED) AS payment_terms,
			si.posting_date AS invoice_posting_date,
		si.outstanding_amount AS invoice_outstanding_amount
		FROM 
			`latest_si_by_sales_order_overdue` si 
			LEFT JOIN `tabCustomer` tc ON tc.name = si.customer 
			WHERE 
			si.docstatus = 1
			AND tc.custom_omc_dealer_code IS NULL
			AND tc.name NOT IN (select * from party_not_include_customer)
			), 
			overdue_amount_group2 as (
			select oag.account_manager,
			ROUND(
			GREATEST(
			SUM(
			CASE WHEN oag.invoice_posting_date <= DATE_SUB(
				CURRENT_DATE(), 
				INTERVAL oag.payment_terms DAY
			) THEN oag.invoice_outstanding_amount ELSE 0 END
			),
			0), 
		2) as overdue_amount, 
		case when ROUND(
			GREATEST(
			SUM(
			CASE WHEN oag.invoice_posting_date <= DATE_SUB(
				CURRENT_DATE(), 
				INTERVAL oag.payment_terms DAY
			) THEN oag.invoice_outstanding_amount ELSE 0 END
			) - IFNULL(pet.total_unallocated_amount, 0)- IFNULL(jvt.total_jv_amount, 0), 
			0), 
		2) >  case when get.total_gl_amount != IFNULL(IFNULL(SUM(oag.invoice_outstanding_amount), 0) - COALESCE(jvt.total_jv_amount, 0)- IFNULL(pet.total_unallocated_amount,0),0) then total_gl_amount else IFNULL(IFNULL(SUM(oag.invoice_outstanding_amount), 0) - COALESCE(jvt.total_jv_amount, 0)- IFNULL(pet.total_unallocated_amount,0),0) end then case when get.total_gl_amount != IFNULL(IFNULL(SUM(oag.invoice_outstanding_amount), 0) - COALESCE(jvt.total_jv_amount, 0)- IFNULL(pet.total_unallocated_amount,0),0) then total_gl_amount else IFNULL(IFNULL(SUM(oag.invoice_outstanding_amount), 0) - COALESCE(jvt.total_jv_amount, 0)- IFNULL(pet.total_unallocated_amount,0),0) end else ROUND(
			GREATEST(
			SUM(
			CASE WHEN oag.invoice_posting_date <= DATE_SUB(
				CURRENT_DATE(), 
				INTERVAL oag.payment_terms DAY
			) THEN oag.invoice_outstanding_amount ELSE 0 END
			) - IFNULL(pet.total_unallocated_amount, 0)- IFNULL(jvt.total_jv_amount, 0), 
			0), 
		2) end as net_overdue
		from overdue_amount_group oag
		left join jv_total jvt on jvt.party = oag.customer_id
		left join gl_entry_total get on get.party = oag.customer_id
		left join payment_entry_total pet on pet.party = oag.customer_id
		where get.total_gl_amount > 0
		group by 1,pet.total_unallocated_amount,jvt.total_jv_amount,get.total_gl_amount),
		overdue_semi as(
			select account_manager,
			sum(overdue_amount) as overdue_amount,
			sum(net_overdue) as net_overdue
			from overdue_amount_group2
			group by 1
		),
		overdue_final as (
			select account_manager,overdue_amount,net_overdue
			from overdue_semi
		),
		net_outstanding_group as (
		select tc.account_manager,
		tc.name as customer_id,
		outstanding_amount
		from latest_si_by_sales_order_overdue si
		LEFT JOIN `tabCustomer` tc ON tc.name = si.customer 
		where si.docstatus = 1
			AND tc.custom_omc_dealer_code IS NULL
			AND tc.name NOT IN (select * from party_not_include_customer)
			), net_outstanding_group2 as (
		select nog.account_manager,
		case when total_gl_amount != IFNULL(IFNULL(SUM(outstanding_amount), 0) - COALESCE(total_jv_amount, 0)- IFNULL(total_unallocated_amount,0),0) then total_gl_amount else IFNULL(IFNULL(SUM(outstanding_amount), 0) - COALESCE(total_jv_amount, 0)- IFNULL(total_unallocated_amount,0),0) end as net_outstanding
		from net_outstanding_group nog
		left join jv_total jvt on jvt.party = nog.customer_id
		left join gl_entry_total get on get.party = nog.customer_id
		left join payment_entry_total pet on pet.party = nog.customer_id
		where get.total_gl_amount > 0
		group by 1,total_gl_amount,total_jv_amount,total_unallocated_amount
		),
		net_outstanding_semi as (
		select account_manager,
		sum(net_outstanding) as net_outstanding_amount
		from net_outstanding_group2
		group by 1
		),
		net_outstanding_final as (
		select 
		account_manager,
		net_outstanding_amount
		from net_outstanding_semi)
		SELECT DISTINCT
				st.business_head AS bushead,
				st.sales_person AS slp,
				sp.custom_team AS team,
				bd.zonal_head as head,
				bd.zonal_head_sales_person as head_name,
				bd.bd AS team_username,
				bd.bd_sales_person AS bd_name,
			round(nos.net_outstanding_amount / 100000,2) as net_outstanding_amount,
			round(case when off.overdue_amount = 0 then off.overdue_amount else off.net_overdue end / 100000 ,2) as net_overdue_amount,
		ROUND(
			CASE 
				WHEN bd.daily_targerts = 0 THEN 0
				ELSE (bd.daily_targerts * 90) / 10000000
			END, 
			2
		) AS lwd_tgt,    
		ROUND(yc.amount / 10000000, 2) AS lwd_actual,
			ROUND(CASE 
				WHEN COALESCE(ROUND((bd.daily_targerts * 90) / 10000000, 2), 0) = 0 THEN 0 
				ELSE (COALESCE(ROUND(yc.amount / 10000000, 2), 0) / COALESCE(ROUND((bd.daily_targerts * 90) / 100000, 2), 0)) * 100 
			END,0) AS lwd_att,
			ROUND((bd.weekly_targets * 90) / 10000000, 2) AS week_tgt,
			ROUND(((bd.weekly_targets *90) / (10000000 * (SELECT working_days_week FROM work_days_summary_week))) * (SELECT days_passed_week FROM work_days_summary_week), 2) AS wtd_tgt,
				ROUND(wc.week_collection / 10000000, 2) AS month_collection,
				ROUND(CASE 
				WHEN COALESCE(ROUND((bd.weekly_targets * 90) / 10000000, 2), 0) = 0 THEN 0 
				ELSE (COALESCE(ROUND(wc.week_collection / 10000000, 2), 0) / ROUND(((bd.weekly_targets * 90) / (10000000 * (SELECT working_days_week FROM work_days_summary_week))) * (SELECT days_passed_week FROM work_days_summary_week), 2)) * 100 
			END,0) AS week_att,
				ROUND((bd.monthly_targets * 90) / 10000000, 2) AS Month_TGT,
			ROUND((bd.monthly_targets * 90)  / (10000000 * (SELECT working_days_month FROM work_days_summary_month)) * (SELECT day_of_month FROM current_day_of_month), 2) AS month_tgt,
			ROUND(mc.month_collection / 10000000, 2) AS month_collection,
					ROUND(CASE 
					WHEN COALESCE(ROUND((bd.monthly_targets * 90) / 10000000, 2), 0) = 0 THEN 0 
					ELSE (COALESCE(ROUND(mc.month_collection / 10000000, 2), 0) / ROUND((bd.monthly_targets * 90) / (10000000 * (SELECT working_days_month FROM work_days_summary_month)) * (SELECT day_of_month FROM current_day_of_month), 2)) * 100 
				END, 0) AS month_att
		FROM ranked_data st
		LEFT JOIN `tabBusiness Deverloper` bd ON bd.parent = st.name
		LEFT JOIN `daily_achivement` da ON da.bd = bd.bd AND da.sales_target = st.name 
		LEFT JOIN `weekly_achivement` wa ON wa.bd = bd.bd AND wa.sales_target = st.name
		LEFT JOIN `monthly_achivement` ma ON ma.bd = bd.bd AND ma.sales_target = st.name
		LEFT JOIN sales_person sp ON sp.custom_user = bd.bd
		LEFT JOIN zonal_target tz ON tz.parent = st.name AND tz.zonal_head = bd.zonal_head
		LEFT JOIN customer_count cc ON cc.account_manager = bd.bd
		left join yesterday_collection yc on yc.account_manager = bd.bd
		left join week_collection wc on wc.account_manager = bd.bd
		left join month_collection mc on mc.account_manager = bd.bd
		left join net_outstanding_final nos on nos.account_manager = bd.bd
		left join overdue_final off on off.account_manager = bd.bd
		WHERE rank = 1
		AND ((bd.zonal_head = %s ) OR (bd.bd = %s) OR (st.business_head = %s) ) GROUP BY bd.bd
    
    """
    
    # Add more logic to process the results of the query and return it as data
    data = frappe.db.sql(query, (frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)
    return data

