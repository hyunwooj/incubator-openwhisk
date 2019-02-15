/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.entity

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import spray.json._
import org.apache.openwhisk.core.ConfigKeys
import pureconfig._

case class GpuLimitConfig(min: Int, max: Int, std: Int)

/**
 * GpuLimit encapsulates allowed gpu for an action. The limit must be within a
 * permissible range (by default [128MB, 512MB]).
 *
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @param count the gpu limit for the action
 */
protected[entity] class GpuLimit private (val count: Int) extends AnyVal

protected[core] object GpuLimit extends ArgNormalizer[GpuLimit] {
  val config = loadConfigOrThrow[GpuLimitConfig](ConfigKeys.gpu)

  protected[core] val minGpu: Int = config.min
  protected[core] val maxGpu: Int = config.max
  protected[core] val stdGpu: Int = config.std

  /** Gets GpuLimit with default value */
  protected[core] def apply(): GpuLimit = GpuLimit(stdGpu)

  /**
   * Creates GpuLimit for limit, iff limit is within permissible range.
   *
   * @param count the limit must be within permissible range
   * @return GpuLimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(count: Int): GpuLimit = {
    require(count >= minGpu, s"$count gpus below allowed threshold of $minGpu")
    require(count <= maxGpu, s"$count gpus exceeds allowed threshold of $maxGpu")
    new GpuLimit(count)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[GpuLimit] {
    def write(g: GpuLimit) = JsNumber(g.count)

    def read(value: JsValue) =
      Try {
        val JsNumber(count) = value
        require(count.isWhole(), "gpu limit must be whole number")
        GpuLimit(count.toInt)
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("gpu limit malformed", e)
      }
  }
}
