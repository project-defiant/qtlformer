nextflow.enable.dsl = 2

include { buildManifest } from './modules/build-manifest.nf'

def intro() {
    log.info(
        """
        QTLFormer Nextflow Pipeline
    """.stripIndent()
    )
}

/*
 * SET UP CONFIGURATION VARIABLES
 */


workflow {

    intro()
    print(params)
    input_ch = channel.fromPath(params.input_dir)
    buildManifest(input_ch)
    workflow.onComplete { log.info("Pipeline complete!") }
}
