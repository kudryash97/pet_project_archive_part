/*В любой БД*/
CREATE DATABASE electro_abramovo;
/*Все последующие запросы в БД electro_abramovo*/
CREATE EXTENSION IF NOT EXISTS pxf;

CREATE TABLE public.data_16 (
	"time" int4,
	"Mcs" int4,
	num_sign int4,
	"data" int2,
	"Pr" int2,
	bstate int2,
	bsrc int2,
	kks_id_signal bpchar(25)
)
WITH
(appendoptimized=true,
compresstype=ZSTD,
compresslevel=5,
orientation=column
)
DISTRIBUTED BY ("time");


CREATE TABLE public.data_85 (
	"time" int4,
	"Mcs" int4,
	num_sign int4,
	"data" int2,
	"Pr" int2,
	bstate int2,
	bsrc int2,
	kks_id_signal bpchar(25)
)
WITH
(appendoptimized=true,
compresstype=ZSTD,
compresslevel=5,
orientation=column
)
DISTRIBUTED BY ("time");


CREATE TABLE public.data_10 (
	"time" int4,
	mcs int4,
	num_sign int4,
	"data" float4,
	bzone int2,
	isevnt int2,
	bstate int2,
	bsrc int2,
	kks_id_signal bpchar(25)
)
WITH
(appendoptimized=true,
compresstype=ZSTD,
compresslevel=5,
orientation=column
)
DISTRIBUTED BY ("time");

CREATE TABLE public.data_80 (
	"time" int4,
	mcs int4,
	num_sign int4,
	"data" float4,
	bzone int2,
	isevnt int2,
	bstate int2,
	bsrc int2,
	kks_id_signal bpchar(25)
)
WITH
(appendoptimized=true,
compresstype=ZSTD,
compresslevel=5,
orientation=column
)
DISTRIBUTED BY ("time");