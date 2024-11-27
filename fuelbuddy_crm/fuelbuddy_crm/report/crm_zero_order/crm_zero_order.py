import frappe
from frappe.utils import nowdate

def execute(filters=None):
    # Define columns
    columns = [
        {"fieldname": "team", "label": "Team", "fieldtype": "Data", "width": 150},
        {"fieldname": "team_username", "label": "BD", "fieldtype": "Data", "width": 150},
        {"fieldname": "zonal_head_sales_person", "label": "Zonal Head", "fieldtype": "Data", "width": 150},
        #{"fieldname": "zonal_head", "label": "Zonal Head", "fieldtype": "Data", "width": 150},
        {"fieldname": "customer_id", "label": "Customer ID", "fieldtype": "Data", "width": 150},
        {"fieldname": "customer_name", "label": "Customer Name", "fieldtype": "Data", "width": 200},
        {"fieldname": "payment_type", "label": "Payment Type", "fieldtype": "Data", "width": 100},
        {"fieldname": "days_since_last_onboarding", "label": "Days Since Onboarding", "fieldtype": "Int", "width": 100},
    ]

    # Fetch filter values
    zonal_head = filters.get("zonal_head") if filters else None
    bd = filters.get("bd") if filters else None
    business_head = filters.get("business_head") if filters else None

    # SQL query
    query = """
    WITH sales_persons AS (
        SELECT 
            name AS team_user_name,
            custom_user, 
            custom_team
        FROM 
            `tabSales Person`
    ), ranked_data AS (
        SELECT 
            business_head,
            posting_date,
            name,
            sales_person,
            ROW_NUMBER() OVER (PARTITION BY business_head ORDER BY posting_date DESC) AS rank
        FROM 
            `tabSales Target`
    ),
    customers_0_orders AS (
        SELECT 
            account_manager AS bd,
            tc.name as customer_id,
            tc.customer_name,
            tc.payment_type,
            DATE(tc.creation) AS onboarding_date,
            DATEDIFF(CURRENT_DATE(), DATE(tc.creation)) AS days_since_last_onboarding
        FROM 
            `tabLead` tl
        LEFT JOIN 
            `tabOpportunity` op ON op.party_name = tl.name
        LEFT JOIN 
            `tabCustomer` tc ON tl.name = tc.lead_name
        LEFT JOIN 
            `tabSales Order` so ON so.customer = tc.name
        WHERE 
            tl.transaction_type = 'High Speed Diesel'
            AND op.party_name IS NOT NULL 
            AND tc.lead_name IS NOT NULL
            AND so.name IS NULL
            AND tc.customer_primary_contact NOT IN (
                'CON-ERP-FY-09064', 'CON-APP-FY-04429', 'CON-APP-FY-04237',
                'CON-APP-FY-04431', 'CON-APP-FY-04301', 'CON-APP-FY-04457',
                'CON-APP-FY-04675', 'CON-ERP-FY-23654', 'CON-APP-FY-000005783',
                'CON-APP-FY-04664', 'CON-APP-FY-04609', 'CON-ERP-FY-09065',
                'CON-APP-FY-04469', 'CON-APP-FY-04237', 'CON-APP-FY-04431',
                'CON-APP-FY-04454', 'CON-ERP-FY-000024587', 'CON-ERP-FY-000024581',
                'CON-ERP-FY-20282', 'CON-ERP-FY-20283', 'CON-ERP-FY-20284',
                'CON-ERP-FY-000024582', 'CON-ERP-FY-20277', 'CON-ERP-FY-000024811',
                'CON-ERP-FY-000024810', 'CON-ERP-FY-000024809', 'CON-APP-FY-000006607',
                'CON-ERP-FY-20282', 'CON-ERP-FY-20284', 'CON-ERP-FY-000024649',
                'CON-ERP-FY-20279', 'CON-ERP-FY-000024586', 'CON-ERP-FY-20288',
                'CON-ERP-FY-000024583', 'CON-ERP-FY-000024584', 'CON-ERP-FY-20281',
                'CON-ERP-FY-20285', 'CON-ERP-FY-10671', 'CON-ERP-FY-000024332',
                'CON-APP-FY-04304', 'CON-APP-FY-000017976', 'CON-ERP-FY-000024852',
                'CON-ERP-FY-000024836', 'CON-APP-FY-000009809', 'CON-ERP-FY-000024346',
                'CON-ERP-FY-000024702'
            )
    )
    SELECT DISTINCT
        sp.custom_team AS Team,
        sp.team_user_name AS Team_Username,
        bd.zonal_head_sales_person,
        bd.zonal_head,
        czo.customer_id,
        czo.customer_name,
        czo.payment_type,
        czo.days_since_last_onboarding
    FROM 
        ranked_data st
    LEFT JOIN 
        `tabBusiness Deverloper` bd ON bd.parent = st.name
    LEFT JOIN 
        sales_persons sp ON sp.custom_user = bd.bd
    LEFT JOIN 
        customers_0_orders czo ON czo.bd = bd.bd
    WHERE 
        st.rank = 1
        AND ((bd.zonal_head = %s) OR (bd.bd = %s) OR (st.business_head = %s))
    GROUP BY 
        bd.bd
    ORDER BY 
        czo.days_since_last_onboarding DESC
    """

    # Fetch data
    data = frappe.db.sql(query,(frappe.session.user, frappe.session.user, frappe.session.user) , as_dict=True)

    # Format data
    formatted_data = [
        {
            "team": row.get("Team"),
            "team_username": row.get("Team_Username"),
            "zonal_head_sales_person": row.get("zonal_head_sales_person"),
            "zonal_head": row.get("zonal_head"),
            "customer_id": row.get("customer_id"),
            "customer_name": row.get("customer_name"),
            "payment_type": row.get("payment_type"),
            "days_since_last_onboarding": row.get("days_since_last_onboarding"),
        }
        for row in data
    ]

    return columns, formatted_data

