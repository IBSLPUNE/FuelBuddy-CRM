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
		WHERE name NOT IN ('Abhishek Anand','Ankur Goel','Narayan Sathe','Nora Bali','Palash Gupta','Ravi','SK Arora','Setu Joshi','Vadivel Nandhyalam')
	),
	cte AS (
	    SELECT DISTINCT 
		td.reference_type,
		td.reference_name,
		td.owner,
		td.name AS Todo_ID,
		CASE 
		    WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND td.status != 'Cancelled' THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name 
		            WHEN td.reference_name = tl.name THEN tl.company_name 
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name 
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name 
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name 
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name 
		                    ELSE ep.reference_docname 
		                END
		            ELSE td.reference_name 
		        END
		    ELSE '-' 
		END AS Yesterday,
		CASE 
		    WHEN date = CURRENT_DATE() AND td.status != 'Cancelled' THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name 
		            WHEN td.reference_name = tl.name THEN tl.company_name 
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name 
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name 
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name 
		                    ELSE ep.reference_docname 
		                END
		            ELSE td.reference_name 
		        END 
		    ELSE '-' 
		END AS Today,
		CASE 
		    WHEN date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AND td.status != 'Cancelled' THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name 
		            WHEN td.reference_name = tl.name THEN tl.company_name 
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name 
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name 
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name 
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name 
		                    ELSE ep.reference_docname 
		                END
		            ELSE td.reference_name 
		        END 
		    ELSE '-' 
		END AS Tomorrow,
		CASE 
		    WHEN EXTRACT(WEEK FROM td.date) = EXTRACT(WEEK FROM CURRENT_DATE()) 
		        AND EXTRACT(YEAR FROM td.date) = EXTRACT(YEAR FROM CURRENT_DATE()) 
		        AND td.status != 'Cancelled' THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name 
		            WHEN td.reference_name = tl.name THEN tl.company_name 
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name 
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name 
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name 
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name 
		                    ELSE ep.reference_docname 
		                END
		            ELSE td.reference_name 
		        END 
		    ELSE '-' 
		END AS Week,
		COUNT(CASE WHEN td.status = 'Open' THEN td.name ELSE NULL END) AS Count_Status_Open
	    FROM 
		`tabToDo` td
	    LEFT JOIN 
		`tabEvent Participants` ep 
		ON ep.parent = td.reference_name AND ep.idx = 1
	    LEFT JOIN 
		`tabCustomer` tc 
		ON tc.name = td.reference_name OR ep.reference_docname = tc.name
	    LEFT JOIN 
		`tabLead` tl 
		ON tl.name = td.reference_name OR ep.reference_docname = tl.name
	    LEFT JOIN 
		`tabOpportunity` tpo 
		ON tpo.name = td.reference_name OR ep.reference_docname = tpo.name
	    WHERE td.status = 'Open'
	    GROUP BY 
		1, 2, 3, 4, 5, 6, 7,8
	), 
	final_cte as (
	SELECT 
	    Todo_ID,
	    reference_name,
	    reference_type,
	    owner,
	    Yesterday,
	    Today,
	    Tomorrow,
	    Week,
	    Count_Status_Open
	FROM cte 
	WHERE 
	    (Yesterday != '-' OR 
	    Today != '-' OR 
	    Tomorrow != '-' OR 
	    Week != '-')
	)
	select distinct
	sp.custom_team AS Team,
	sp.team_user_name AS BD,
	bd.zonal_head_sales_person AS Zonal_head,
	case when reference_type = 'Event' then reference_name else Todo_ID end as Todo_ID,
	case when reference_type = 'Event' then 'Event' else 'Task' end as Appointment_type,
	reference_type AS Reference_Type,
	todo.Yesterday,
	todo.Today,
	todo.Week,
	todo.Count_Status_Open
	from ranked_data st
	left join `tabBusiness Deverloper` bd on bd.parent = st.name
	LEFT JOIN sales_person sp ON sp.custom_user = bd.bd
	left join final_cte todo on todo.owner = bd.bd
	where rank = 1
	and (Yesterday != '-' OR 
	    Today != '-' OR 
	    Tomorrow != '-' OR 
	    Week != '-')
      AND ((bd.zonal_head = %s) OR (bd.bd = %s) OR (st.business_head = %s))
    order by Team,BD
    """
    
    # Execute the query
    data = frappe.db.sql(query,(frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)

    # Define the columns
    columns = [
        {"fieldname": "Team", "label": "Team Name", "fieldtype": "Data", "width": 150},
        #{"fieldname": "Team_Username", "label": "Team Username", "fieldtype": "Data", "width": 150},
        {"fieldname": "BD", "label": "BD Name", "fieldtype": "Data", "width": 200},
        {"fieldname": "zonal_head_sales_person", "label": "Zonal Head", "fieldtype": "Data", "width": 150},
        {"fieldname": "Todo_ID", "label": "Todo ID", "fieldtype": "Data", "width": 150},
        {"fieldname": "Appointment_type", "label": "Appointment Type", "fieldtype": "Data", "width": 200},
        {"fieldname": "Reference_Type", "label": "Reference Type", "fieldtype": "Data", "width": 150},
        {"fieldname": "Yesterday", "label": "Yesterday", "fieldtype": "Data", "width": 150},
        {"fieldname": "Today", "label": "Today", "fieldtype": "Data", "width": 200},
        {"fieldname": "Week", "label": "Week", "fieldtype": "Data", "width": 150},
        {"fieldname": "Count_Status_Open", "label": "Count Status Open ", "fieldtype": "Data", "width": 200}
        
    ]
    
    # Return columns and data
    return columns, data

