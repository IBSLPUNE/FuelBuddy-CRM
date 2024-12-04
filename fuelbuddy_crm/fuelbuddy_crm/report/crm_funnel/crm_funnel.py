import frappe

def execute(filters=None):
    # Query to fetch the data
    query = """
    WITH ranked_data AS (
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
    ),
    open_lead as (
        SELECT 
            lead_owner as bd,
            COUNT(DISTINCT tl.name) as open_leads,
            COUNT(CASE WHEN tl.hwc = 'Hot' THEN tl.name ELSE NULL END) as hot_leads,
            COUNT(CASE WHEN tl.hwc = 'Warm' THEN tl.name ELSE NULL END) as warm_leads,
            COUNT(CASE WHEN tl.hwc = 'Cold' THEN tl.name ELSE NULL END) as cold_leads,
            COUNT(CASE WHEN tl.hwc IS NULL OR tl.hwc = "" THEN tl.name ELSE NULL END) as not_marked_leads
        FROM `tabLead` tl
        LEFT JOIN `tabOpportunity` op ON op.party_name = tl.name
        LEFT JOIN `tabCustomer` tc ON tl.name = tc.lead_name
        WHERE tl.transaction_type = 'High Speed Diesel'
          AND op.party_name IS NULL 
          AND tc.lead_name IS NULL
        GROUP BY 1
    ),
    opportunity_stages as (
        SELECT 
            tl.lead_owner as bd,
            COUNT(op.name) as opportunity_leads,
            COUNT(CASE WHEN activity_ = '1.In Discussion Meeting Pending' THEN op.name ELSE NULL END) as In_Discussion_Meeting_Pending,
            COUNT(CASE WHEN activity_ = '2.Meeting Scheduled' THEN op.name ELSE NULL END) as Meeting_Scheduled,
            COUNT(CASE WHEN activity_ = '3.In Negotitation' THEN op.name ELSE NULL END) as In_Negotitation,
            COUNT(CASE WHEN activity_ = '4.Finance Approval Pending' THEN op.name ELSE NULL END) as Finance_Approval_Pending,
            COUNT(CASE WHEN activity_ = '5.Ops Submission Pending' THEN op.name ELSE NULL END) as Ops_Submission_Pending,
            COUNT(CASE WHEN activity_ = '6.Ops Approval Pending' THEN op.name ELSE NULL END) as Ops_Approval_Pending,
            COUNT(CASE WHEN activity_ = '7.Ops approved- Finance closure Pending' THEN op.name ELSE NULL END) as Ops_approved_Finance_closure_Pending,
            COUNT(CASE WHEN activity_ = '8.Lost' THEN op.name ELSE NULL END) as Lost,
            COUNT(CASE WHEN activity_ = '9.On-boarded' THEN op.name ELSE NULL END) as Onboarded
        FROM `tabLead` tl
        LEFT JOIN `tabOpportunity` op ON op.party_name = tl.name
        LEFT JOIN `tabCustomer` tc ON tl.name = tc.lead_name
        WHERE tl.transaction_type = 'High Speed Diesel'
          AND op.party_name IS NOT NULL
          AND tc.lead_name IS NULL
        GROUP BY 1
    ),
    customer_onboarding as (
        SELECT 
            account_manager as bd,
            DATEDIFF(CURRENT_DATE(), MAX(DATE(creation))) AS days_since_last_onboarding,
            MAX(DATE(creation)) as last_onboarding_date, 
            COUNT(name) as total_customers
        FROM `tabCustomer` 
        WHERE transaction_type = 'High Speed Diesel'
        GROUP BY 1
    ),
    customers_0_orders as (
        SELECT 
            account_manager as bd,
            COUNT(tl.name) as customers_0_orders
        FROM `tabLead` tl
        LEFT JOIN `tabOpportunity` op ON op.party_name = tl.name
        LEFT JOIN `tabCustomer` tc ON tl.name = tc.lead_name
        LEFT JOIN `tabSales Order` so ON so.customer = tc.name
        WHERE tl.transaction_type = 'High Speed Diesel'
          AND op.party_name IS NOT NULL 
          AND tc.lead_name IS NOT NULL
          AND so.name IS NULL
        GROUP BY 1
    )
    SELECT DISTINCT
        sp.custom_team AS Team,
        sp.team_user_name AS Team_Username,
        bd.bd_sales_person AS BD,
        bd.zonal_head_sales_person,
        bd.zonal_head,
        open_leads,
        hot_leads,
        cold_leads,
        warm_leads,
        not_marked_leads,
        opportunity_leads,
        In_Discussion_Meeting_Pending,
        Meeting_Scheduled,
        In_Negotitation,
        Finance_Approval_Pending,
        Ops_Submission_Pending,
        Ops_Approval_Pending,
        Ops_approved_Finance_closure_Pending,
        Lost,
        Onboarded,
        days_since_last_onboarding,
        total_customers,
        customers_0_orders
    FROM ranked_data st
    LEFT JOIN `tabBusiness Deverloper` bd ON bd.parent = st.name
    LEFT JOIN sales_person sp ON sp.custom_user = bd.bd
    LEFT JOIN open_lead ol ON ol.bd = bd.bd
    LEFT JOIN opportunity_stages os ON os.bd = bd.bd
    LEFT JOIN customer_onboarding co ON co.bd = bd.bd
    LEFT JOIN customers_0_orders czo ON czo.bd = bd.bd
    WHERE rank = 1
      AND ((bd.zonal_head = %s) OR (bd.bd = %s) OR (st.business_head = %s))
    GROUP BY 
        bd.bd
    """
    
    # Execute the query
    data = frappe.db.sql(query,(frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)

    # Define the columns
    columns = [
        {"fieldname": "Team", "label": "Team Name", "fieldtype": "Data", "width": 150},
        #{"fieldname": "Team_Username", "label": "Team Username", "fieldtype": "Data", "width": 150},
        {"fieldname": "zonal_head_sales_person", "label": "Zonal Head", "fieldtype": "Data", "width": 150},
        {"fieldname": "BD", "label": "BD Name", "fieldtype": "Data", "width": 200},
        {"fieldname": "open_leads", "label": "Open Leads", "fieldtype": "Data", "width": 150},
        {"fieldname": "hot_leads", "label": "Hot Leads", "fieldtype": "Data", "width": 200},
        {"fieldname": "warm_leads", "label": "Warm Leads", "fieldtype": "Data", "width": 150},
        {"fieldname": "cold_leads", "label": "Cold Leads", "fieldtype": "Data", "width": 150},
        {"fieldname": "not_marked_leads", "label": "Not Marked", "fieldtype": "Data", "width": 200},
        {"fieldname": "opportunity_leads", "label": "Opportunity Leads", "fieldtype": "Data", "width": 150},
        {"fieldname": "In_Discussion_Meeting_Pending", "label": "In discussion meeting pending ", "fieldtype": "Data", "width": 250},
        {"fieldname": "Meeting_Scheduled", "label": "Meeting scheduled", "fieldtype": "Data", "width": 200},
        {"fieldname": "In_Negotitation", "label": "In negotiation", "fieldtype": "Data", "width": 200},
        {"fieldname": "Finance_Approval_Pending", "label": "Finance approval pending", "fieldtype": "Data", "width": 250},
        {"fieldname": "Ops_Submission_Pending", "label": "Ops submission pending", "fieldtype": "Data", "width": 250},
        {"fieldname": "Ops_Approval_Pending", "label": "Ops approval pending", "fieldtype": "Data", "width": 250},
        {"fieldname": "Ops_approved_Finance_closure_Pending", "label": "Ops approved finance closure pending", "fieldtype": "Data", "width": 250},
        {"fieldname": "Lost", "label": "Lost", "fieldtype": "Data", "width": 200},
        {"fieldname": "Onboarded", "label": "Onboarded", "fieldtype": "Data", "width": 150},
        {"fieldname": "days_since_last_onboarding", "label": "Days since last onboarding", "fieldtype": "Data", "width": 250},
        {"fieldname": "customers_0_orders", "label": "Customers with 0 orders", "fieldtype": "Data", "width": 250}
        
    ]
    
    # Return columns and data
    return columns, data

