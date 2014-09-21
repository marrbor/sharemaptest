/*
 *   Copyright (c) Genetec Corporation. All Rights Reserved.
 */

package jp.survei.sharemaptest

import jp.survei.base.BusMod

import org.vertx.java.core.Future

class StartUp extends BusMod {

  def spec = [:]

  @Override def start(Future<Void> sr) {
    super.start()
    logger.info "Start StartUp"
    def confresult = chkconfig(config, spec)   // verify configuration.
    logger.debug "chkconfig returns ${confresult}."
    if (confresult) sr.setFailure(confresult) // something wrong.
    else {
      container.deployVerticle("groovy:jp.survei.sharemaptest.Producer") {
        if (!it.succeeded) sr.setFailure(new Exception(it.cause()))
        else {
          container.deployVerticle("groovy:jp.survei.sharemaptest.Consumer") {
            if (!it.succeeded) sr.setFailure(new Exception(it.cause()))
            else sr.setResult(null)
          }
        }
      }
    }
  }

  @Override def stop() {
    logger.info "StartUp Stopped."
  }
}
