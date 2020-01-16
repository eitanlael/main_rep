--create new indexes at table   con_alloc_transaction  
    CREATE INDEX nzh_data.con_alloc_transaction_nu1 ON nzh_data.con_alloc_transaction(nvl(supp_scnd_first_part, 0)) TABLESPACE indx;
    CREATE INDEX nzh_data.con_alloc_transaction_nu2 ON nzh_data.con_alloc_transaction(supp_first_part_cons_no) TABLESPACE indx;
    CREATE INDEX nzh_data.con_alloc_transaction_nu3 ON nzh_data.con_alloc_transaction(first_part_cons_no) TABLESPACE indx;   
  --  CREATE INDEX con_alloc_transaction_nu4 ON con_alloc_transaction(SCND_FIRST_PART_CONS_NO) TABLESPACE indx;
   CREATE INDEX nzh_data.con_alloc_transaction_nu5 ON nzh_data.con_alloc_transaction(YEAR) TABLESPACE indx;

	
