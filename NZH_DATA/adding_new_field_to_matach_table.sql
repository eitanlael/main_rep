--run all script from system user
--iiהוספת שדה לטבלת מט"שים
--fir test of git

--run this in STG_DATA
ALTER TABLE STG_DATA.DIM_SEW_TREAT_DEVICES
ADD ID_NO	VARCHAR2(10 BYTE);

--run this in NZH_MAIN
alter table NZH_MAIM.DIM_SEW_TREAT_DEVICES
add ID_NO	VARCHAR2(10 BYTE);

--run this package in ETL_MGR
create or replace PACKAGE BODY         ETL_MGR.DEVICES_DWH AS 
/******************************************************************************
   NAME:       DIVICES_DWH 
   PURPOSE: DIVICES_DWH (אסדרה קר"מ) KARAM

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ---------------------d---------------
   1.0        15/05/2014  Leonid           1. Created this package.
   1.1        01/09/2014  Tamar            1. Add fields to FACT_METER_POSITION_CHANGES
   2.0        27/10/2014  Tamar            1. Add Procedure Dim_Device_Groups
                                           2. Add Procedure insert_Dim_Device_Groups
   3.0       21/04/2015   Tamar            1. Add Procedure Fact_Device_Details
                                           2. Add Procedure Insert_Fact_Device_Details
   5.0       05/05/2017                    Insert Dim_prod_device_group   
   6.0       20/08/2017    Alexander       1. Add function F_Get_Prod_Dev_Type_Code
                                           2. Changed procedure Fact_Device_Details
                                           
   7.0       06/12/2017    Alexander       1. Changed procedure Fact_Device_Details
   8.0       20/01/2018    Leonid          1. Add Table Dim_Device_Technical_Equip 
   9.0       23/03/2018    Leonid          add new table Dim_Sew_Treat_Devices,Dim_Sew_Owners   
   10.0      03/05/2018    Leonid          1.Add Dim assets   
   10.0      03/05/2018    SM              Changes in proc. Fact_Device_Details, Insert_Fact_Device_Details. 
   11.0      25/12/2019    Eitan          Add column id_no to DIM_SEW_TREAT_DEVICES and populate it
******************************************************************************/
     err_code  number;
     err_msg   varchar2(200);

     v_etl_date     date            ;
     v_err_proc     varchar2(100)   ;



    Procedure Load_Stg_Tables
    IS

    BEGIN

     --- set etl date by subject
        Etl_Batch.Set_Etl_Date(c_subject_id) ;
        V_Etl_Date:= Etl_Batch.Get_Etl_Date(c_subject_id) ;
     --   End Etl Date    -----------------------------

     --dim_projects
     v_err_proc:=' Dim_Meter_Position ' ;
     Dim_Meter_Position ;

     -- Fact_Meter_Position_Changes
     v_err_proc:=' Fact_Meter_Position_Changes ' ;
     Fact_Meter_Position_Changes  ;

     -- Dim_Device_Groups
     v_err_proc:=' Dim_Device_Groups ' ;
     Dim_Device_Groups  ;
    
     -- Fact_Device_Details
     v_err_proc:=' Fact_Device_Details ' ;
     Fact_Device_Details  ;
    
     -- Dim_Prod_Device_Groups
     v_err_proc:=' Dim_Prod_Device_Groups ' ;
     Dim_Prod_Device_Groups  ;
     
     -- Dim_Device_Technical_Equip
     v_err_proc:=' Dim_Device_Technical_Equip '  ;
     Dim_Device_Technical_Equip  ;
     
     --Dim_Sew_Treat_Devices
     v_err_proc:=' Dim_Sew_Treat_Devices ' ;
     Dim_Sew_Treat_Devices ;
    
     --Dim_Sew_Owners
     v_err_proc:=' Dim_Sew_Owners ' ;
     Dim_Sew_Owners ;
     
     --Dim_Assets
     v_err_proc:=' Dim_Assets ' ;
     Dim_Assets ;
     
     COMMIT ;

     exception
    when others then
         err_code := sqlcode;
         err_msg := substr(sqlerrm, 1, 200);
         etl_batch.insert_err_log (c_batch_id,3,
         'Err IN - '||upper(v_err_proc)||' exec Etl_Tzapa.load_stg_tables',err_code,err_msg);
         raise;

            ROLLBACK ;

    END Load_Stg_Tables;


    Procedure Fact_Meter_Position_Changes
    is
    begin

     INSERT INTO STG_DATA.FACT_METER_POSITION_CHANGES
         (water_source_id,
          position_id,
          assemb_date,
          lab_id,
          lab_name,
          catalog_met_no,
          replaced_cat_met_no,
          report_date,
          first_read,
          last_read,
          replace_date,
          lab_replaced_yn,
          last_read_rep_by_cd,
          last_test_date,
          postpone_reason,
          next_test_date,
          lab_doc_no,
          met_type_cd,
          met_type_desc,
          met_diameter,
          cons_replace_cd,
          arad_update_date,
          etl_date
          )
    SELECT water_source_id,
          position_id,
          assemb_date,
          lab_id,
          Etl_Mgr.CodeshortDesc ('CON_LABORATORY',lab_id) as Lab_Name,
          catalog_met_no,
          replaced_cat_met_no,
          report_date,
          first_read,
          last_read,
          replace_date,
          lab_replaced_yn,
          last_read_rep_by_cd,
          last_test_date,
          postpone_reason,
          next_test_date,
          lab_doc_no,
          met_type_cd,
          Etl_Mgr.CodeshortDesc ('MET_TYPE',met_type_cd) AS Met_Type_Desc,
          met_diameter,
          cons_replace_cd,
          arad_update_date,
          v_etl_date
     FROM NZH_DATA.CON_CURRENT_METER ;

     dbms_output.put_line ('Fact_Meter_Position_Changes '||sql%rowcount);
            --COMMIT;

    end Fact_Meter_Position_Changes;

    ---------------------------
    function f_get_met_name(p_water_source_id number,p_Position_id number) return string
    -- calculate composite Meter Name
     IS
    v_met_name      varchar2(40) ;
    v_met_name_add  varchar2(40) ;
    R_Meter NZH_DATA.CON_CUR_METER_POSITION%ROWTYPE ;
    
    BEGIN    
    
    select * into R_Meter
    FROM NZH_DATA.CON_CUR_METER_POSITION
    WHERE water_source_id=p_water_source_id and Position_id=p_Position_id;
    
    v_met_name:=R_Meter.Met_Name ;
    IF      R_Meter.POSITION_REAL_FICTITIOUS='REAL' THEN
    
    --return v_met_name;
    
      BEGIN  
        
        SELECT   met_name
            into v_met_name_add 
        from NZH_DATA.CON_CUR_METER_POSITION 
        where position_real_fictitious='FICTV' 
        and f_position_id    =R_meter.position_id
        and f_water_source_id=R_meter.water_source_id;
        
        return v_met_name_add ;
        
      EXCEPTION   
        WHEN NO_DATA_FOUND THEN
        
            return '';--R_Meter.met_name ;
            
       END ; 
        
    ELSIF   R_Meter.POSITION_REAL_FICTITIOUS='FICTV' THEN
    
      BEGIN      
    
       SELECT   met_name
         into   v_met_name_add 
        from NZH_DATA.CON_CUR_METER_POSITION 
        where position_real_fictitious='REAL' 
        and position_id    =R_meter.F_position_id
        and water_source_id=R_meter.F_water_source_id;
        
        return v_met_name_add ;
        
      EXCEPTION   
        WHEN NO_DATA_FOUND THEN
            return '';--R_Meter.met_name ;
       END ; 
    else
         return '' ;
    END IF;
    
    return '' ;
        
    END f_get_met_name;
    ---------------------------


    Procedure Dim_Meter_Position
    is
    
    ---------  End Function ------------------    
   
    begin
    
     INSERT INTO STG_DATA.DIM_METER_POSITION
            ( water_source_id,
              position_id,
              met_name,
              lisence_met_type_cd,
              lisence_met_type_desc,
              lisence_met_diameter,
              x_coord,
              y_coord,
              calc_cd,
              calc_desc,
              f_water_source_id,
              f_position_id,
              position_real_fictitious,
              f_met_name,
              etl_date) 
   ( SELECT water_source_id,
           position_id,
           met_name,
           lisence_met_type_cd,
           Etl_Mgr.CodeshortDesc ('MET_TYPE',lisence_met_type_cd),--lisence_met_type_desc,
           lisence_met_diameter,
           x_coord,
           y_coord,
           calc_cd,
           Etl_Mgr.CodeshortDesc ('CALC',calc_cd),--lisence_met_type_desc,
           f_water_source_id,   --leonid 2016/07 add madim fictivi
           f_position_id,
           position_real_fictitious,
           f_get_met_name(water_source_id,position_id) AS F_M_NAME,
           --select * from NZH_DATA.CON_CUR_METER_POSITION where WATER_SOURCE_ID IN (311995,231252) and POSITION_REAL_FICTITIOUS='FICTV'
           v_etl_date
      FROM NZH_DATA.CON_CUR_METER_POSITION);

     dbms_output.put_line ('Dim_Meter_Position '||sql%rowcount);
        -- COMMIT;

    end  Dim_Meter_Position;

    Procedure Dim_Device_Groups
    is
    begin
    INSERT INTO STG_DATA.DIM_DEVICE_GROUPS
            (water_source_id,
            group_id,
            group_name,
            group_type_cd,
            group_type_desc,
            etl_date)
    SELECT  d.water_source_id,
            d.group_id,
            g.group_name,
            g.group_type_cd,
            app_codes_tab_q.GetCodeDesc('GROUP_TYPE',g.group_type_cd) as group_type_desc,
            v_etl_date 
    FROM   NZH_DATA.GEN_GROUP_DETAILS d, NZH_DATA.GEN_DEV_SOURCE_GROUP g
        where  d.GROUP_ID = g.GROUP_ID
        and    g.GROUP_TYPE_CD  not in (8, 9);

    dbms_output.put_line ('Dim_Device_Groups'||sql%rowcount);
        -- COMMIT;

    End Dim_Device_Groups;
    
     Procedure Dim_Prod_Device_Groups
     AS
     BEGIN
     
   
      INSERT INTO STG_DATA.DIM_PROD_DEVICE_GROUPS 
        (group_id,
         water_source_id,
         group_name,
         remarks,
         etl_date)
     SELECT 
          m.group_id,
          m.water_source_id,
          g.group_name,
          m.remarks,
          v_etl_date
     FROM NZH_DATA.CON_DETAILED_DEV_METER_GROUP M 
     RIGHT OUTER JOIN  NZH_DATA.CON_DEV_METER_GROUP G 
      ON M.GROUP_ID=G.GROUP_ID    
      ORDER BY  m.group_id,
          m.water_source_id ;       
      
      dbms_output.put_line ('Dim_Prod_Device_Groups '||sql%rowcount);
      
     END  Dim_Prod_Device_Groups;

    -- Alexander 20/08/2017
    Function F_Get_Prod_Dev_Type_Code(p_code stg_data.appcode.code%TYPE) RETURN stg_data.appcode.code%TYPE -- calculate composite Meter Name
    IS
        v_code stg_data.appcode.code%TYPE;
    
    BEGIN    
    
        SELECT COUNT(*) 
        INTO v_code
        FROM stg_data.APPCODESPRODDEVTYPE
        WHERE code = p_code;
        
        IF p_code IS NULL or v_code <> 0 THEN
            RETURN   p_code; 
        ELSE
            RETURN 99;  
        END IF;
        
    END;    
    
 Procedure Dim_Device_Technical_Equip 
  AS
    BEGIN
     
     INSERT INTO STG_DATA.DIM_DEVICE_TECHNICAL_EQUIP 
        (water_source_id,
         pump_id,
         pump_diameter,
         pump_produc,
         pump_serial_no,
         rmp,
         engine_type,
         engine_produc,
         eng_serial_no,
         horse_power,
         hour_discharge,
         manomet_pressure,
         voltage,
         test_date,
         assem_date,
         REMARK,
         pump_type,
         calc_hour_discharge,
         etl_date)
     SELECT water_source_id,
          pump_id,
          pump_diameter,
          pump_produc,
          pump_serial_no,
          rmp,
          engine_type,
          engine_produc,
          eng_serial_no,
          horse_power,
          hour_discharge,
          manomet_pressure,
          voltage,
          test_date,
          assem_date,
          REMARK,
          pump_type,
          calc_hour_discharge,
          v_etl_date
         FROM NZH_DATA.CON_TECHNICAL_EQUIP
         --ORDER BY water_source_id, pump_id
          ;
         
         dbms_output.put_line ('Dim_Device_Technical_Equip '||Sql%RowCount);
          
 END Dim_Device_Technical_Equip ;
    
    
    Procedure Fact_Device_Details
    IS
    BEGIN
    INSERT INTO STG_DATA.Fact_Device_Details
            (year,
            water_source_id,
            --first_part_cons_no, -- Canc. by SM 20/06/19
            consumer_id,
            water_group_id,
            is_mekorot_yn,
            is_mekorot_yn_desc,
            hh_num,
            prod_dev_type_cd,
            prod_dev_type_desc,
            dev_status_cd,   -- Alexander 06/12/2017
            dev_status_desc, -- Alexander 06/12/2017
            water_borehole_type_cd,   -- Leonid 06/01/2018
            water_borehole_type_desc, 
            etl_date)
    SELECT  a.year, 
            a.water_source_id, 
            --a.first_part_cons_no, -- Canc. by SM 20/06/19 
            --to_number (a.first_part_cons_no || LPAD (a.second_part_cons_no, 5, '0')) as consumer_id,
            ---nvl( to_number (a.first_part_cons_no || LPAD (a.second_part_cons_no, 5, '0')), 99999900000) as consumer_id, -- SM 23/05/19
            nvl( etl_mgr.F_Get_Consumer_Id (a.first_part_cons_no,a.second_part_cons_no), 99999900000) as consumer_id, -- SM 03/07/19
            a.water_group_id,
            devices.GetIsMekorotYN(a.year,a.first_part_cons_no,a.second_part_cons_no) AS is_mekorot_yn, -- Alexander 20/08/2017
            decode(devices.GetIsMekorotYN(a.year,a.first_part_cons_no,a.second_part_cons_no),'Y','מקורות','N','צרכן פרטי','בעלות לא ידועה') as is_mekorot_yn_desc, -- Alexander 20/08/2017
            a.hh_num,
            F_Get_Prod_Dev_Type_Code(substr(a.hh_num,1,2)) as prod_dev_type_cd,
            etl_mgr.CodeShortDesc('PROD_DEV_TYPE',F_Get_Prod_Dev_Type_Code(substr(a.hh_num,1,2))) as prod_dev_type_desc,
            a.dev_status_cd,  -- Alexander 06/12/2017 
            etl_mgr.CodeShortDesc('DEV_STATUS',a.dev_status_cd) as dev_status_desc, -- Alexander 06/12/2017
            a.water_borehole_type_cd,   -- Leonid 06/01/2018
             etl_mgr.CodeShortDesc('WATER_BOREHOLE_TYPE', a.water_borehole_type_cd) as water_borehole_type_desc, 
             v_etl_date 
    FROM CON_DEVICES_YEAR a
    WHERE a.year <= to_number (to_char (trunc (sysdate), 'YYYY'));


    dbms_output.put_line ('Fact_Device_Details '||sql%rowcount);
         --COMMIT;
         
    end Fact_Device_Details;
    
     Procedure Insert_Dim_Sew_Treat_Devices 
     as
     BEGIN
     
        INSERT INTO NZH_MAIM.DIM_SEW_TREAT_DEVICES
        (   water_source_id ,          
            sew_status_cd  ,           
            sew_status_desc ,          
            municip_first_part_cons_no,
            treat_id      ,            
            treat_heb_name   ,         
            sew_qual_cd   ,            
            sew_qual_desc  ,           
            sew_license_yn ,           
            operator_id    ,           
            operator_name   ,          
            contact_person ,           
            remark        ,            
            add_year    ,              
            add_ser_no  ,              
            settle_id   ,              
            name       ,               
            street     ,               
            house_no   ,               
            zip_code   ,               
            pob         ,              
            pob_settle_id  ,           
            pob_settle_name  ,         
            pob_zip      ,             
            tel_1        ,             
            tel_2         ,            
            fax          ,             
            e_mail      ,              
          etl_date ,
          id_no)
         select 
          water_source_id ,          
            sew_status_cd  ,           
            sew_status_desc ,          
            municip_first_part_cons_no,
            treat_id      ,            
            treat_heb_name   ,         
            sew_qual_cd   ,            
            sew_qual_desc  ,           
            sew_license_yn ,           
            operator_id    ,           
            operator_name   ,          
            contact_person ,           
            remark        ,            
            add_year    ,              
            add_ser_no  ,              
            settle_id   ,              
            name       ,               
            street     ,               
            house_no   ,               
            zip_code   ,               
            pob         ,              
            pob_settle_id  ,           
            pob_settle_name  ,         
            pob_zip      ,             
            tel_1        ,             
            tel_2         ,            
            fax          ,             
            e_mail      ,              
          etl_date,
           id_no
         FROM STG_DATA.DIM_SEW_TREAT_DEVICES ;
         
     END Insert_Dim_Sew_Treat_Devices
     ;
     
     Procedure Insert_Dim_Sew_Owners 
    AS
    
    BEGIN
     
     INSERT INTO NZH_MAIM.DIM_SEW_OWNERS
        (
          consumer_id ,
          water_source_id ,
          first_part_cons_no ,
          cons_name ,
          etl_date        
        )
        SELECT 
          consumer_id ,
          water_source_id ,
          first_part_cons_no ,
          cons_name ,
          etl_date  
        FROM STG_DATA.DIM_SEW_OWNERS; 
        
     END  Insert_Dim_Sew_Owners ; 
     
       Procedure Insert_Dim_Assets
        AS
    
      BEGIN
      
       INSERT INTO NZH_MAIM.DIM_ASSETS 
           (water_source_id,
            merkava_num,
            responsible,
            source_type_cd,
            source_type_desc,
            order_number, -- SM 15/08/19
            direct_cost, -- SM 15/08/19
            indirect_cost, -- SM 15/08/19
            pay_responsible, -- SM 15/08/19
            etl_date)
       select water_source_id,
            merkava_num,
            responsible,
            source_type_cd,
            source_type_desc,
            order_number, -- SM 15/08/19
            direct_cost, -- SM 15/08/19
            indirect_cost, -- SM 15/08/19
            pay_responsible, -- SM 15/08/19
            V_etl_Date
       FROM STG_DATA.DIM_ASSETS;
        
    END  Insert_Dim_Assets;
    
    PROCEDURE Load_Maim_Tables
    AS
    
    BEGIN
    
     -- Meter_Pos_Changes
     v_err_proc:=' Insert_Fact_Meter_Pos_Changes ' ;
     Insert_Fact_Meter_Pos_Changes ;

     -- Dim_Meter_Position
     v_err_proc:=' Insert_Dim_Meter_Position ' ;
     Insert_Dim_Meter_Position      ;

     -- Dim_Device_Groups
     v_err_proc:=' Insert_Dim_Device_Groups ' ;
     Insert_Dim_Device_Groups       ;
    
     -- Fact_Device_Details
     v_err_proc:=' Insert_Fact_Device_Details ' ;
     Insert_Fact_Device_Details     ;
   
     -- Insert_Prod_Dim_Device_Groups
     v_err_proc:=' Insert_Dim_Prod_Device_Groups ' ;
     Insert_Dim_Prod_Device_Groups  ;
    
     -- Insert_Dim_Device_Tech_Equip   
	 v_err_proc:=' Insert_Dim_Device_Tech_Equip ' ;
     Insert_Dim_Device_Tech_Equip   ;     
     
     --Insert_Dim_Sew_Treat_Devices
     v_err_proc:=' Insert_Dim_Sew_Treat_Devices ' ;
     Insert_Dim_Sew_Treat_Devices           ;
      
     --Insert_Dim_Sew_Owners
     v_err_proc:=' Insert_Dim_Sew_Owners ' ;
     Insert_Dim_Sew_Owners                 ;
      
      --Insert_Dim_Assets
     v_err_proc:=' Insert_Dim_Assets ' ;
     Insert_Dim_Assets                 ;
           
     COMMIT; 
    
    EXCEPTION 
      WHEN OTHERS  THEN 
       
        err_code := SQLCODE;
        err_msg := SUBSTR (SQLERRM, 1, 200);
        etl_batch.insert_err_log (c_batch_id, 4, 'Exec Devices_DWH.Load_Maim_Table '||
        v_err_proc,err_code, err_msg);
           ROLLBACK ;
       RAISE;
  
    END Load_Maim_Tables;
    
    
    Procedure Insert_Fact_Meter_Pos_Changes
    is
    begin
    
        begin

            INSERT INTO NZH_MAIM.FACT_METER_POSITION_CHANGES
            ( water_source_id,
              position_id,
              assemb_date,
              lab_id,
              lab_name,
              catalog_met_no,
              replaced_cat_met_no,
              report_date,
              first_read,
              last_read,
              replace_date,
              lab_replaced_yn,
              last_read_rep_by_cd,
              last_test_date,
              postpone_reason,
              next_test_date,
              lab_doc_no,
              met_type_cd,
              met_type_desc,
              met_diameter,
              cons_replace_cd,
              arad_update_date,
              etl_date)
                SELECT water_source_id,
                  position_id,
                  assemb_date,
                  lab_id,
                  lab_name,
                  catalog_met_no,
                  replaced_cat_met_no,
                  report_date,
                  first_read,
                  last_read,
                  replace_date,
                  lab_replaced_yn,
                  last_read_rep_by_cd,
                  last_test_date,
                  postpone_reason,
                  next_test_date,
                  lab_doc_no,
                  met_type_cd,
                  met_type_desc,
                  met_diameter,
                  cons_replace_cd,
                  arad_update_date,
                  etl_date
             FROM STG_DATA.FACT_METER_POSITION_CHANGES;
             /*
               exception
                when others then
                     err_code := sqlcode;
                     err_msg := substr(sqlerrm, 1, 200);
                     etl_batch.insert_err_log(c_batch_id,5,'exec devices_dwh.Insert_Fact_Meter_Pos_Changes',err_code,err_msg);
                     raise;
                     */
            end;


           -- COMMIT;

    end  Insert_Fact_Meter_Pos_Changes;

    procedure Insert_Dim_Meter_Position
    is
    begin
            begin

             INSERT INTO    NZH_MAIM.DIM_METER_POSITION
            ( water_source_id,
              position_id,
              met_name,
              lisence_met_type_cd,
              lisence_met_type_desc,
              lisence_met_diameter,
              x_coord,
              y_coord,
              calc_cd,
              calc_desc,
              f_water_source_id,
              f_position_id,
              position_real_fictitious,
              f_met_name,
              etl_date)
            SELECT
              water_source_id,
              position_id,
              met_name,
              lisence_met_type_cd,
              lisence_met_type_desc,
              lisence_met_diameter,
              x_coord,
              y_coord,
              calc_cd,
              calc_desc,
              f_water_source_id,
              f_position_id,
              position_real_fictitious,
              f_met_name,
              etl_date
            FROM   STG_DATA.DIM_METER_POSITION
             ;
                /*
                exception
                when others then
                     err_code := sqlcode;
                     err_msg := substr(sqlerrm, 1, 200);
                     etl_batch.insert_err_log (c_batch_id,4,'exec devices_dwh.Insert_Dim_Meter_Position',err_code,err_msg);
                     raise;
                     */
            end;

           -- commit;
    end  Insert_Dim_Meter_Position;
    
      Procedure Dim_Sew_Treat_Devices
    AS
    
    BEGIN
     
     INSERT INTO STG_DATA.DIM_SEW_TREAT_DEVICES
        (   water_source_id ,          
            sew_status_cd  ,           
            sew_status_desc ,          
            municip_first_part_cons_no,
            treat_id      ,            
            treat_heb_name   ,         
            sew_qual_cd   ,            
            sew_qual_desc  ,           
            sew_license_yn ,           
            operator_id    ,           
            operator_name   ,          
            contact_person ,           
            remark        ,            
            add_year    ,              
            add_ser_no  ,              
            settle_id   ,              
            name       ,               
            street     ,               
            house_no   ,               
            zip_code   ,               
            pob         ,              
            pob_settle_id  ,           
            pob_settle_name  ,         
            pob_zip      ,             
            tel_1        ,             
            tel_2         ,            
            fax          ,             
            e_mail      ,              
           etl_date     ,
           id_no        )
         select 
            T.water_source_id,
            T.sew_status_cd,
            etl_mgr.CodeShortDesc('SEW_STATUS', T.sew_status_cd) As sew_status_desc,
            T.municip_first_part_cons_no,
            T.treat_id,
           (select heb_treat_desc From  NZH_DATA.GEN_SEW_TREAT s where s.treat_id=t.treat_id )as treat_heb_name,
           T.sew_qual_cd,
           etl_mgr.CodeShortDesc('SEW_QUALITY', T.sew_qual_cd) As sew_qual_desc,
           T.license_yn,
           O.operator_id,
           O.operator_name,
           O.contact_person,
           O.Remark ,
           EXTRACT(YEAR FROM  SYSDATE) As add_year,
           o.add_ser_no, 
           ADDR.Settle_id, 
           ADDR. name, 
           ADDR. street, 
           ADDR. house_no, 
           ADDR.zip_code, 
           ADDR.pob, 
           ADDR.pob_settle_id, 
           ADDR.pob_settle_name, 
           ADDR.pob_zip, 
           ADDR.tel_1, 
           ADDR.tel_2, 
           ADDR.fax, 
           ADDR.e_mail,
           V_Etl_Date,
           o.ID_NO
                 FROM (
                   SELECT A.ADD_SER_NO,
                   a.pob_settle_id as settle_id,
                   a. name, 
                   a. street, 
                   a. house_no, 
                   a.zip_code, 
                   a.pob    , 
                   a.pob_settle_id, 
                   (select l.settle_name from NZH_MAIM.DIM_SETTLES L where l.settle_id=a.pob_settle_id )as pob_settle_name, 
                   a.pob_zip, 
                   a.tel_1  , 
                   a.tel_2  , 
                   a.fax    , 
                   a.e_mail from NZH_DATA.CON_ADDRESS_YEAR A where A.year=EXTRACT(YEAR FROM  SYSDATE) 
                   ) ADDR  ,
                    NZH_DATA.CON_SEW_TREAT_DEV T  ,
                    NZH_DATA.CON_SEW_OPERATORS O 
         WHERE      T.sew_operator =O.OPERATOR_ID (+)
               AND  O.ADD_SER_NO=ADDR.ADD_SER_NO  (+)    
               --AND T.WATER_SOURCE_ID=313670
           ;
           --ALTERNATIVE ANSI FOR SQLSERVER
          /* select 
            T.water_source_id,
            T.sew_status_cd,
            etl_mgr.CodeShortDesc('SEW_STATUS', T.sew_status_cd) As sew_status_desc,
            T.municip_first_part_cons_no,
            T.treat_id,
           (select heb_treat_desc From  NZH_DATA.GEN_SEW_TREAT s where s.treat_id=t.treat_id )as treat_heb_name,
           T.sew_qual_cd,
           etl_mgr.CodeShortDesc('SEW_QUALITY', T.sew_qual_cd) As sew_qual_desc,
           T.license_yn,
           O.operator_id,
           O.operator_name,
           O.contact_person,
           O.Remark ,
           EXTRACT(YEAR FROM  SYSDATE) As add_year,
           o.add_ser_no, 
           ADDR.Settle_id, 
           ADDR. name, 
           ADDR. street, 
           ADDR. house_no, 
           ADDR.zip_code, 
           ADDR.pob, 
           ADDR.pob_settle_id, 
           (select l.settle_name from NZH_MAIM.DIM_SETTLES L where l.settle_id=addr.pob_settle_id )as pob_settle_name , 
           ADDR.pob_zip, 
           ADDR.tel_1, 
           ADDR.tel_2, 
           ADDR.fax, 
           ADDR.e_mail,
           sysdate
                 FROM    NZH_DATA.CON_SEW_TREAT_DEV T   
                    LEFT OUTER JOIN NZH_DATA.CON_SEW_OPERATORS O   ON T.sew_operator =O.OPERATOR_ID
                    LEFT OUTER JOIN NZH_DATA.CON_ADDRESS_YEAR ADDR ON O.ADD_SER_NO   =ADDR.ADD_SER_NO 
                    and ADDR.YEAR=EXTRACT(YEAR FROM  SYSDATE) 
                    --                    WHERE T.WATER_SOURCE_ID=313670*/
           --FROM NZH_DATA.CON_SEW_TREAT_DEV T,NZH_DATA.CON_ADDRESS_YEAR A,NZH_DATA.CON_SEW_OPERATORS O
           --WHERE  T.sew_operator =O.OPERATOR_ID(+) and 
           --T.SEW_SETTLE_ID=A.SETTLE_ID(+)
            dbms_output.put_line ('STG_DATA.Dim_Sew_Treat_Devices '||sql%rowcount);
    END  Dim_Sew_Treat_Devices;
    
    
 --   NZH_DATA.CON_ADDRESS_YEAR
    
    Procedure Dim_Sew_Owners 
    AS
    
    BEGIN
    
     INSERT INTO STG_DATA.DIM_SEW_OWNERS
        (
          consumer_id ,
          water_source_id ,
          first_part_cons_no ,
          cons_name ,
          etl_date        
        )
        SELECT 
        o.first_part_cons_no  ||'00000' as consumer_id,
        o.water_source_id,
        o.first_part_cons_no,
        (select cons_name from STG_DATA.DIM_CONSUMERS C 
            where o.first_part_cons_no  ||'00000'= c.consumer_id ) as cons_name,
        v_etl_date
        FROM NZH_DATA.CON_SEW_OWNERS O;
        
         dbms_output.put_line ('STG_DATA.DIM_SEW_OWNERS '||sql%rowcount);
    END  Dim_Sew_Owners;
            
    Procedure Dim_Assets
        AS
    
    BEGIN
    
       INSERT INTO STG_DATA.DIM_ASSETS 
           (water_source_id,
            merkava_num,
            responsible,
            source_type_cd,
            source_type_desc,
            order_number, -- SM 15/08/19
            direct_cost, -- SM 15/08/19
            indirect_cost, -- SM 15/08/19
            pay_responsible, -- SM 15/08/19
            etl_date)
  select water_source_id,
            merkava_num,
            responsible,
            source_type_cd,
            Etl_Mgr.CodeshortDesc ('WATER_SOURCE_TYPE',source_type_cd) as assource_type_desc,
            order_number, -- SM 15/08/19
            direct_cost, -- SM 15/08/19
            indirect_cost, -- SM 15/08/19
            pay_responsible, -- SM 15/08/19
            V_etl_Date
  FROM NZH_DATA.HDR_ASSETS;
        
        dbms_output.put_line ('Dim_Assets '||sql%rowcount);
            
    END  Dim_Assets;

    Procedure Insert_Dim_Device_Groups
    is
    begin
        begin
        INSERT INTO NZH_MAIM.DIM_DEVICE_GROUPS
            (water_source_id,
            group_id,
            group_name,
            group_type_cd,
            group_type_desc,
            etl_date)
        SELECT  water_source_id,
            group_id,
            group_name,
            group_type_cd,
            group_type_desc,
            etl_date
        FROM   STG_DATA.DIM_DEVICE_GROUPS;
        
        dbms_output.put_line ('STG_DATA.DIM_DEVICE_GROUPS '||sql%rowcount);
        /*
        exception
                when others then
                     err_code := sqlcode;
                     err_msg := substr(sqlerrm, 1, 200);
                     etl_batch.insert_err_log (c_batch_id,6,'exec devices_dwh.Insert_Dim_Device_Groups',err_code,err_msg);
                     raise;
                     */
            end;

      -- commit;

    End Insert_Dim_Device_Groups;

    Procedure Insert_Dim_Prod_Device_Groups
    AS
    
    BEGIN
    
     INSERT INTO NZH_MAIM.DIM_PROD_DEVICE_GROUPS 
        (group_id,
         water_source_id,
         group_name,
         remarks,
         etl_date)
     SELECT 
         group_id,
         water_source_id,
         group_name,
         remarks,
         etl_date
         FROM STG_DATA.DIM_PROD_DEVICE_GROUPS  ;
    
    END  Insert_Dim_Prod_Device_Groups;
    
Procedure Insert_Dim_Device_Tech_Equip 
    AS
    
  BEGIN
    
     INSERT INTO NZH_MAIM.DIM_DEVICE_TECHNICAL_EQUIP 
        (water_source_id,
         pump_id,
         pump_diameter,
         pump_produc,
         pump_serial_no,
         rmp,
         engine_type,
         engine_produc,
         eng_serial_no,
         horse_power,
         hour_discharge,
         manomet_pressure,
         voltage,
         test_date,
         assem_date,
         REMARK,
         pump_type,
         calc_hour_discharge,
         etl_date)
     SELECT 
         water_source_id,
         pump_id,
         pump_diameter,
         pump_produc,
         pump_serial_no,
         rmp,
         engine_type,
         engine_produc,
         eng_serial_no,
         horse_power,
         hour_discharge,
         manomet_pressure,
         voltage,
         test_date,
         assem_date,
         REMARK,
         pump_type,
         calc_hour_discharge,
         etl_date
     FROM STG_DATA.DIM_DEVICE_TECHNICAL_EQUIP ; 
    
 END  Insert_Dim_Device_Tech_Equip ;
    
    

    Procedure Insert_Fact_Device_Details
    IS
    BEGIN
        begin
        INSERT INTO NZH_MAIM.Fact_Device_Details
                (year,
                    water_source_id,
                    --first_part_cons_no, -- Canc. by SM 23/05/19
                    consumer_id,
                    water_group_id,
                    is_mekorot_yn,
                    is_mekorot_yn_desc,
                    hh_num,
                    prod_dev_type_cd,
                    prod_dev_type_desc,
                    dev_status_cd,   -- Alexander 06/12/2017
                    dev_status_desc, -- Alexander 06/12/2017
                    water_borehole_type_cd,   -- Leonid 06/01/2018
                    water_borehole_type_desc,
                    etl_date)
        SELECT  year,
                water_source_id,
                --first_part_cons_no, -- Canc. by SM 23/05/19
                consumer_id,
                water_group_id,
                is_mekorot_yn,
                is_mekorot_yn_desc,
                hh_num,
                prod_dev_type_cd,
                prod_dev_type_desc,
                dev_status_cd,   -- Alexander 06/12/2017
                dev_status_desc, -- Alexander 06/12/2017
                water_borehole_type_cd,   -- Leonid 06/01/2018
                water_borehole_type_desc,
                etl_date 
        FROM STG_DATA.Fact_Device_Details;

        /*
        exception
                    when others then
                         err_code := sqlcode;
                         err_msg := substr(sqlerrm, 1, 200);
                         etl_batch.insert_err_log (c_batch_id,7,'exec devices_dwh.Insert_Fact_Device_Details ',err_code,err_msg);
                         raise;
                         */
          end;
        -- COMMIT;
         
    End Insert_Fact_Device_Details;

END ETL_MGR.DEVICES_DWH;

--END OF SCRIPTS



