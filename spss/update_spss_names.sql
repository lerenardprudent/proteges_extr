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
SET @dunno := "??";

UPDATE form_part_elem_inputs fpei2 JOIN
(
SELECT
  @idsbj                                                                                           AS idsubj,
  @heading_row := idsubj = @headingid                                                              AS heading_row,
  @answered := fr.id IS NOT null                                                                   AS answered,
  @nominal := fpe.type_resp_inputs IN ('cb', 'r', 'c') AND
                (fpei.text_fr IS NOT null OR fpei.val IS NOT null) AND
                fpei.name NOT like '%nom' AND fpei.name NOT like '%name'                           AS nominal,
  @ith := fpei.val                                                                                 AS ith,
  @addprecis := fpei.type_fld_details IS NOT null                                                  AS addprecis,
  @varname_suffix := IF(@nominal, CONCAT(@spacer, @ith), '')                                       AS varname_suffix,
  @qname := CONCAT(fpei.name, @varname_suffix)                                                     AS qualname,
  @name2 := IF(@nominal, @qname, fpei.name)                                                        AS name2,
  @shortened_name := PREG_REPLACE('/([^_])[aeiou]/', "$1",
                           REPLACE(@name2, 'comportement_avec_partenaires_statut', 'caps'))        AS shortened,
  @namey := IF(CHAR_LENGTH(@name2) >= @minvarlengforshortening, @shortened_name, @name2)           AS namey,
  @namey_pr := CONCAT(@namey, @varprecissuffix)                                                    AS nameypr,
  @text := IF( fpei.text_fr IS NOT null, fpei.text_fr, IF(fpei.pos IS NOT null, fpei.pos, @dunno)) AS text,
  @texte := IF(@answered, REPLACE(@text, @sep, @sepreplacement), @blank)                           AS texte,
  @safeval := IF(@answered, REPLACE(fr.val, @sep, @sepreplacement), @blank)                        AS safeval,
  @safeprecis := IF(@answered, REPLACE(fr.resp_precision, @sep, @sepreplacement), @blank)          AS precis,
  @reponse := IF(@nominal, @texte, @safeval)                                                       AS rpnse,
  f.form_date                                                                                      AS date_formulaire,
  f.visit_month                                                                                    AS mois_visite,
  fpe.type_resp_inputs, fp.part_idx, fpe.elem_idx, fpei.id, fpei.name, fpei.pos, fpei.val
FROM
(SELECT id, part_idx FROM form_parts WHERE form_type_id = 1) fp
JOIN form_part_elems fpe ON fpe.form_part_id = fp.id AND fpe.type_resp_inputs IS NOT null
JOIN form_part_elem_inputs fpei ON fpei.form_part_elem_id = fpe.id
JOIN (SELECT id, idsubj FROM patients WHERE idsubj = @idsbj) p
LEFT JOIN forms f ON f.form_type_id = 1 AND f.patient_id = p.id
LEFT JOIN form_responses fr ON fr.form_id = f.id AND fr.form_part_elem_input_id = fpei.id
WHERE 1
ORDER BY part_idx, elem_idx, pos, val
) x
ON fpei2.id = x.id
SET fpei2.spss_var_name = x.namey
