*
* safeappend.ado: append whenever varnames match, but don't delete data
*

*first define a tostring programs that will simply create a new variable is it does not create a string first time.

cap program drop safeappend2
program define safeappend2
    version 12
    syntax using [, List DRYrun]
	
quietly { // no output from intermediate commands

* -- save current state -- *
tempfile master
save `master'

/*  -------------------------
    Loop over master dataset and post varnames and types
    ------------------------- */
* postfile config 
tempname postvars
tempfile mastervars 
postfile `postvars' str32 varname str7 mastertype using `mastervars'

* loop and post
use `master'
local mastertype "" // string or numeric
foreach var of varlist _all {
    capture confirm numeric variable `var'
    if _rc { // string
        local mastertype "string"
    }
    else { //numeric
        local mastertype "numeric"
    }
    post `postvars' ("`var'") ("`mastertype'")
}

* wrap up
postclose `postvars'
clear

use `mastervars'

/*  -------------------------
    Loop over using dataset and post varnames and types
    ------------------------- */

* postfile config 
tempfile usingvars 
postfile `postvars' str32 varname str7 usingtype using `usingvars'

* loop and post
use `using'
local usingtype "" // string or numeric
foreach var of varlist _all {
    capture confirm numeric variable `var'
    if _rc { // string
        local usingtype "string"
    }
    else { //numeric
        local usingtype "numeric"
    }
    post `postvars' ("`var'") ("`usingtype'")
}

* wrap up
postclose `postvars'
clear

use `usingvars'

/*  -------------------------
    Merge posted datasets for using and master
    ------------------------- */
use `mastervars' 
merge 1:1 varname using `usingvars', keep(match) 
    // TODO: preserve order of variable names
drop _merge

* list differences if requested
if !missing("`list'") {
noisily {
    di
    di "Variables with string/numeric conflict:"
    list if usingtype != mastertype, noobs ab(10)
}
}

* dummy variables if tostringing needed
gen num_master_only = 1 if mastertype == "numeric" & usingtype == "string"
gen num_using_only = 1 if usingtype == "numeric" & mastertype == "string"

/*  -------------------------
    "write" programs to tostring master, using, then run the append command
    ------------------------- */

tempfile tostring_using tostring_master

gen cmd = "global tostringvars " + char(36) + "tostringvars " + varname
outsheet cmd using `tostring_master' if !missing(num_master_only), noq non
outsheet cmd using `tostring_using' if !missing(num_using_only), noq non

tempfile master_safe
use `master', clear

global tostringvars
do `tostring_master'
if "$tostringvars" != "" {
	foreach var of var $tostringvars {
		tostring `var', replace
		cap confirm `var' str
		if _rc {
			noisily {
				di
				di "Could not tostring `var' in master dataset. Generating `var'_SA".
			}
			rename `var' `var'_SA
			local lab : var lab `var'_SA
			tostring `var'_SA, gen(`var') force
			label var `var' "`lab'"
			label var `var'_SA "OLD FORMAT: `lab'"
		}
	}
}	
save `master_safe'

tempfile using_safe
use `using', clear
global tostringvars
do `tostring_using'
if "$tostringvars" != "" {
	foreach var of var $tostringvars {
		tostring `var', replace
		cap confirm `var' str
		if _rc {
			noisily {
				di
				di "Could not tostring `var' in using dataset. Generating `var'_SA".
			}
			rename `var' `var'_SA
			local lab : var lab `var'_SA
			tostring `var'_SA, gen(`var') force
			label var `var' "`lab'"
			label var `var'_SA "OLD FORMAT: `lab'"
		}
	}
}	
save `using_safe'



/*  -------------------------
    Actually append
    ------------------------- */
if missing("`dryrun'") {
    use `master_safe'
    append using `using_safe'
}
else {
    * restore initial dataset
    use `master'
}

} // quietly
end


