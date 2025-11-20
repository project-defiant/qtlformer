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
    manifest_ch = buildManifest(input_ch)
    manifest_ch.view()
    datasets = manifest_ch.splitCsv(sep: '\t', header: true)
    datasets.view()



    workflow.onComplete { log.info("Pipeline complete!") }
}
