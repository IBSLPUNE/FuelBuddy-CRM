import frappe
from frappe.utils import cint, flt

def execute(filters=None):
    # Define columns for the report
    columns = [
        {"label": "Team", "fieldname": "custom_team", "fieldtype": "Data", "width": 150},
        {"label": "BD", "fieldname": "team_username", "fieldtype": "Data", "width": 150},
        {"label": "Zonal Head Sales Person", "fieldname": "zonal_head_sales_person", "fieldtype": "Data", "width": 150},
        {"label": "Zonal Head", "fieldname": "zonal_head", "fieldtype": "Data", "width": 150},
        {"label": "Customer ID", "fieldname": "customer_id", "fieldtype": "Data", "width": 120},
        {"label": "Customer Name", "fieldname": "customer_name", "fieldtype": "Data", "width": 180},
        {"label": "Payment Type", "fieldname": "customer_payment_type", "fieldtype": "Data", "width": 100},
        {"label": "Credit Limit", "fieldname": "credit_limit", "fieldtype": "Currency", "width": 100},
        {"label": "Payment Terms", "fieldname": "payment_terms", "fieldtype": "Int", "width": 80},
        {"label": "Last Delivery City", "fieldname": "last_delivery_city", "fieldtype": "Data", "width": 150},
        {"label": "Credit Breach", "fieldname": "allowed_credit_breach", "fieldtype": "Data", "width": 100},
        {"label": "Ledger", "fieldname": "ledger", "fieldtype": "Currency", "width": 120},
        {"label": "Net Outstanding", "fieldname": "net_outstanding", "fieldtype": "Currency", "width": 120},
        {"label": "Net Overdue", "fieldname": "net_overdue", "fieldtype": "Currency", "width": 120},
        {"label": "Overdue Amount Tomorrow", "fieldname": "overdue_amount_tomorrow", "fieldtype": "Currency", "width": 120},
        {"label": "Overdue Amount This Week", "fieldname": "overdue_amount_this_week", "fieldtype": "Currency", "width": 120},
    ]

    # Query to fetch the data
    query = """
	  WITH latest_si_by_sales_order AS (
	  SELECT 
	    si.sales_order, 
	    si.name, 
	    si.posting_date, 
	    si.posting_time, 
	    si.outstanding_amount, 
	    si.status, 
	    si.customer, 
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
	latest_si_by_customer AS (
	  SELECT 
	    si.sales_order, 
	    si.name, 
	    si.posting_date, 
	    si.posting_time, 
	    si.outstanding_amount, 
	    si.status, 
	    si.customer, 
	    si.shipping_address_name, 
	    si.docstatus,
	    ROW_NUMBER() OVER (
	      PARTITION BY si.customer 
	      ORDER BY 
		si.posting_date DESC, 
		si.posting_time DESC
	    ) AS rn 
	  FROM 
	    `tabSales Invoice` si
	), 
	latest_invoice_adddress AS (
	  SELECT 
	    si.customer, 
	    tAdd.city 
	  FROM 
	    latest_si_by_customer si 
	    LEFT JOIN `tabAddress` tAdd ON tAdd.name = si.shipping_address_name 
	  WHERE 
	    si.rn = 1
	), 
	latest_payment_entry AS (
	  SELECT 
	    tPe.party AS customer, 
	    MAX(tPe.posting_date) AS last_payment_entry_date 
	  FROM 
	    `tabPayment Entry` tPe 
	  GROUP BY 
	    tPe.party
	),
	party_not_include_customer AS (   
	   SELECT distinct parent as customer_code FROM `tabParty Account`
	WHERE 
	     account IN("Trade Receivable for Inactive - TSL","Trade Receiavable for Franchisee - TSL")),
	gl_entry_totals AS (
	    SELECT 
	    party AS customer_id,
	    SUM(debit - credit) AS total_gl_amount
	FROM 
	    `tabGL Entry` AS glEntry
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
	  GROUP BY
	    party
	),
	jv_totals AS (
	  SELECT
	    tJea.party AS customer_id,
	    SUM(credit - debit) AS total_jv_amount
	  FROM `tabJournal Entry` tJe
	  LEFT JOIN `tabJournal Entry Account` tJea ON tJea.parent = tJe.name 
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
	  GROUP BY
	    tJea.party
	),
	payment_entry_totals AS (
	  SELECT
	    tPe.party AS customer_id,
	    SUM(tPe.unallocated_amount) AS total_unallocated_amount
	  FROM
	    `tabPayment Entry` tPe
	  WHERE
	    tPe.party IS NOT NULL AND
	    tPe.docstatus =1
	  GROUP BY 
	    tPe.party
	),
	sale_invoices_details AS (
	 SELECT DISTINCT si.name AS invoiceId, 
	    si.status, 
	    si.outstanding_amount
	    FROM 
	    `latest_si_by_sales_order` si 
	    where si.docstatus = 1
	   
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
	sales_person AS (
	    SELECT name AS team_user_name,
		   custom_user, custom_team
	    FROM `tabSales Person`
	    where  name not in ('Abhishek Anand','Ankur Goel','Narayan Sathe','Nora Bali','Palash Gupta','Ravi','SK Arora','Setu Joshi','Vadivel Nandhyalam')
	),
	cte AS (
	  SELECT 
	    DISTINCT si.name AS invoice_id, 
	    si.status AS invoice_status, 
	    si.outstanding_amount AS invoice_outstanding_amount, 
	    si.posting_date AS invoice_posting_date, 
	    si.posting_date2,
	    sid.outstanding_amount AS outstandingAmount, 
	    customer.name AS customer_id, 
	    customer.customer_name AS customer_name, 
	    customer.payment_type AS customer_payment_type, 
	    customer.custom_lead_owner AS lead_owner, 
	    customer.account_manager, 
	    ccl.credit_limit, 
	    tPe.last_payment_entry_date AS payment_entry_posting_date, 
	    lia.city AS last_delivery_city, 
	CAST(REGEXP_REPLACE(customer.payment_terms, '[^0-9]', '') AS UNSIGNED) AS payment_terms, 
	    CASE WHEN ccl.bypass_credit_limit_check = 1 THEN "Y" ELSE "N" END allowed_credit_breach 
	  FROM 
	    `latest_si_by_sales_order` si 
	    LEFT JOIN `tabCustomer` customer ON customer.name = si.customer 
	    LEFT JOIN `tabLead` lead ON lead.name = customer.lead_name 
	    LEFT JOIN `tabOpportunity` opp ON opp.name = customer.opportunity_name 
	    LEFT JOIN `tabContact` contact ON contact.name = opp.contact 
	    LEFT JOIN `tabCustomer Credit Limit` ccl ON ccl.parent = customer.name 
	    LEFT JOIN `latest_payment_entry` tPe ON tPe.customer = si.customer 
	    LEFT JOIN `latest_invoice_adddress` lia ON lia.customer = si.customer 
	    LEFT JOIN `sale_invoices_details` sid ON sid.invoiceId = si.name
	  WHERE 
	    si.docstatus = 1
	    AND customer.custom_omc_dealer_code IS NULL
	    AND customer.name NOT IN (select * from party_not_include_customer)
	),final_cte as (
	SELECT 
	  cte.customer_id, 
	  cte.customer_name, 
	  cte.customer_payment_type, 
	  cte.lead_owner, 
	  cte.account_manager as bd, 
	  cte.credit_limit, 
	  cte.payment_terms, 
	  cte.last_delivery_city, 
	  cte.allowed_credit_breach, 
	  tGl.total_gl_amount as ledger,
	  tJe.total_jv_amount as adjustment,
	  tPe.total_unallocated_amount as unsettled_amount, 
	  SUM(cte.outstandingAmount) AS outstanding_amount,
	  IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) AS net_outstanding_amount, 
	    case when tGl.total_gl_amount != IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) then total_gl_amount else IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) end as net_outstanding2,
	  ROUND(
	    GREATEST(
	      SUM(
	      CASE WHEN cte.invoice_posting_date <= DATE_SUB(
		CURRENT_DATE(), 
		INTERVAL cte.payment_terms DAY
	      ) THEN cte.invoice_outstanding_amount ELSE 0 END
	    ),
	    0), 
	  2) as overdue_amount, 

	    ROUND(
		GREATEST(
		    SUM(
		        CASE 
		            WHEN cte.invoice_posting_date = DATE_SUB(
		                DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), 
		                INTERVAL cte.payment_terms DAY
		            ) THEN cte.invoice_outstanding_amount 
		            ELSE 0 
		        END
		    ),
		0), 
	    2) as overdue_amount_tomorrow,
	ROUND(
	    GREATEST(
		SUM(
		    CASE 
		        WHEN cte.invoice_posting_date BETWEEN 
		            DATE_SUB(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL cte.payment_terms DAY) 
		            AND DATE_SUB(DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY), INTERVAL cte.payment_terms DAY) 
		        THEN cte.invoice_outstanding_amount 
		        ELSE 0 
		    END
		),
	    0), 
	2) AS overdue_amount_this_week,
	    case when ROUND(
	    GREATEST(
	      SUM(
	      CASE WHEN cte.invoice_posting_date <= DATE_SUB(
		CURRENT_DATE(), 
		INTERVAL cte.payment_terms DAY
	      ) THEN cte.invoice_outstanding_amount ELSE 0 END
	    ) - IFNULL(tPe.total_unallocated_amount, 0)- IFNULL(tJe.total_jv_amount, 0), 
	    0), 
	  2) >  case when tGl.total_gl_amount != IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) then total_gl_amount else IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) end then case when tGl.total_gl_amount != IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) then total_gl_amount else IFNULL(IFNULL(SUM(cte.outstandingAmount), 0) - COALESCE(tJe.total_jv_amount, 0)- IFNULL(tPe.total_unallocated_amount,0),0) end else ROUND(
	    GREATEST(
	      SUM(
	      CASE WHEN cte.invoice_posting_date <= DATE_SUB(
		CURRENT_DATE(), 
		INTERVAL cte.payment_terms DAY
	      ) THEN cte.invoice_outstanding_amount ELSE 0 END
	    ) - IFNULL(tPe.total_unallocated_amount, 0)- IFNULL(tJe.total_jv_amount, 0), 
	    0), 
	  2) end as net_overdue2
	FROM 
	  cte 
	  LEFT JOIN `gl_entry_totals` tGl ON tGl.customer_id = cte.customer_id
	  LEFT JOIN `jv_totals` tJe ON tJe.customer_id = cte.customer_id
	  LEFT JOIN `payment_entry_totals` tPe ON tPe.customer_id = cte.customer_id
	WHERE tGl.total_gl_amount > 0
	GROUP BY 
	  1, 
	  2, 
	  3, 
	  4, 
	  5, 
	  6, 
	  7, 
	  8, 
	  9,
	  10, 
	  11, 
	  12
	)
	select distinct
	sp.custom_team AS Team,
	sp.team_user_name AS Team_Username,
	bd.zonal_head_sales_person,
	bd.zonal_head,
	customer_id, 
	customer_name, 
	customer_payment_type,  
	credit_limit, 
	payment_terms, 
	last_delivery_city, 
	allowed_credit_breach, 
	ledger,
	net_outstanding2 as net_outstanding,
	case when overdue_amount = 0 then 0 else net_overdue2 end as net_overdue,
	overdue_amount_tomorrow,
	overdue_amount_this_week
	from ranked_data st
	left join `tabBusiness Deverloper` bd on bd.parent = st.name
	left join final_cte fc  on fc.bd = bd.bd
	LEFT JOIN sales_person sp ON sp.custom_user = fc.bd
	where rank = 1
	and customer_payment_type = 'Postpaid'
	AND (
        (bd.zonal_head = %s )
        OR (bd.bd = %s)                     
        OR (st.business_head = %s)
      	)
    """
    
    # Execute the query
    data = frappe.db.sql(query, (frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)

    return columns, data

