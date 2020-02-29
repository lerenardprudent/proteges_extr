SET SESSION group_concat_max_len=99999;
SET @headingid := "000000";
SET @idsbj := @headingid;
SET @sep := ';';
SET @sepreplacement := @sep;
SET @rowterminator := 'FIN';
SET @varprecissuffix := '_pr';
SET @blank := '';
SET @spacer := '__';
SET @spssvarlengmax := 64;
SET @worstcasevarqualifier := CONCAT(@spacer, "99", @varprecissuffix);
SET @minvarlengforshortening := @spssvarlengmax - CHAR_LENGTH(@worstcasevarqualifier);
SET @ith := 1;
SET @vn := "";
SET @pvn := "";
SET @dunno := "??";
SET @formtype := 1;

SELECT
  IF(heading_row, "idsubj", idsubj)                   AS idsubj,
  IF(heading_row, "date_formulaire", date_formulaire) AS date_formulaire,
  IF(heading_row, "mois_visite", mois_visite)         AS mois_visite,
  CONCAT(
    GROUP_CONCAT(
      IF(heading_row,
        IF(addprecis, CONCAT(namey, @sep, nameypr), namey),
        IF(addprecis, CONCAT(rpnse, @sep, precis),  rpnse)
      )
      ORDER BY part_idx, elem_idx, pos, val, name
      SEPARATOR ';'
    ), @sep, @rowterminator) AS reponses
FROM
(
SELECT
  @idsbj                                                                                           AS idsubj,
  @heading_row := idsubj = @headingid                                                              AS heading_row,
  @answered := fr.id IS NOT null                                                                   AS answered,
  @nominal := fpe.type_resp_inputs IN ('cb', 'r', 'c') AND
                (fpei.text_fr IS NOT null OR fpei.val IS NOT null) AND
                fpei.name NOT like '%nom' AND fpei.name NOT like '%name'                           AS nominal,
  @pvn := @vn,
  @vn := fpei.name,
  @ith := fpei.val                                                                                 AS ith,
  @addprecis := fpei.type_fld_details IS NOT null                                                  AS addprecis,
  @varname_suffix := IF(@nominal, CONCAT(@spacer, @ith), '')                                       AS varname_suffix,
  @qname := CONCAT(fpei.name, @varname_suffix)                                                     AS qualname,
  @name2 := IF(@nominal, @qname, fpei.name)                                                        AS name2,
  @shortened_name := REGEXP_REPLACE(
			REPLACE(@name2, 'comportement_avec_partenaires_statut', 'caps'),
			'([^_])[aeiou]', "$1")        						   AS shortened,
  @namey := IF(CHAR_LENGTH(@name2) >= @minvarlengforshortening, @shortened_name, @name2)           AS namey,
  @namey_pr := CONCAT(@namey, @varprecissuffix)                                                    AS nameypr,
  @text := IF( fpei.text_fr IS NOT null, fpei.text_fr,
             IF(fpei.val IS NOT null, fpei.val, IF(fpei.pos IS NOT null, fpei.pos, @dunno)))       AS text,
  @texte := IF(@answered, REPLACE(@text, @sep, @sepreplacement), @blank)                           AS texte,
  @safeval := IF(@answered, REPLACE(fr.val, @sep, @sepreplacement), @blank)                        AS safeval,
  @safeprecis := IF(@answered, REPLACE(fr.resp_precision, @sep, @sepreplacement), @blank)          AS precis,
  @reponse := IF(@nominal, @texte, @safeval)                                                       AS rpnse,
  f.form_date                                                                                      AS date_formulaire,
  f.visit_month                                                                                    AS mois_visite,
  fpe.type_resp_inputs, fp.part_idx, fpe.elem_idx, fpei.id, fpei.name, fpei.pos, fpei.val,
  fpei.spss_var_name
FROM
(SELECT id, part_idx FROM form_parts WHERE form_type_id = @formtype) fp
JOIN form_part_elems fpe ON fpe.form_part_id = fp.id AND fpe.type_resp_inputs IS NOT null
JOIN form_part_elem_inputs fpei ON fpei.form_part_elem_id = fpe.id
JOIN (SELECT id, idsubj FROM patients WHERE idsubj = @idsbj) p
LEFT JOIN forms f ON f.form_type_id = 1 AND f.patient_id = p.id
LEFT JOIN form_responses fr ON fr.form_id = f.id AND fr.form_part_elem_input_id = fpei.id
WHERE 1
) x
GROUP BY idsubj
