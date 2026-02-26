# AGENTS.md — Numaflow AI Agent Guidelines

> **Version:** 4.0  
> **Last Updated:** 2026-02-26  
> **Applies To:** All AI agents working on this repository  
> **Companion File:** See `CLAUDE.md` for the lean version.

---

## Security

### Owners & Contact
- **Security Team:** #collab-platform-security on Slack.
- **Manual Security Review:** Changes to pipeline processing logic, data handling, API/server endpoints, K8s manifests, RBAC, or Dockerfile → **request security review**.

### Quickstart
1) Identify: **Go Pipeline Logic**, **API/Server**, **K8s Manifests**, **Dockerfile**.  
2) Apply **Security Invariants** to every change.  
3) CRITICAL → **block**. HIGH/MEDIUM/LOW → **flag with fix**.

**Tags:** `[MUST]` = required | `[REDLINE]` = forbidden | `[SHOULD]` = best practice

### Security Invariants (Always Apply)
- **[MUST] No secrets in code or logs** (credentials, tokens, API keys).
- **[MUST] Validate all pipeline inputs** — untrusted data must be sanitized before processing.
- **[MUST] Data isolation between pipelines** — no cross-pipeline data leakage.
- **[MUST] Pin supply chain:** Go deps→exact versions, images→**version/SHA**, no `latest`.
- **[MUST] All code in approved GitHub organizations** (AtlanHQ).

#### Secret Discovery Protocol
If you discover a secret → treat as **CRITICAL**. Do NOT commit/push. Flag with 🔒 SECURITY REVIEW. Recommend rotation. Notify Security team.

### Code Type Security Matrix

| Code Type | Key Risks |
|-----------|-----------|
| **Go Pipeline Logic** | Data injection, cross-pipeline leakage, resource exhaustion |
| **API/Server** | Auth bypass, input validation, information disclosure |
| **K8s Manifests** | Excessive RBAC, privilege escalation, resource limits |
| **Dockerfile** | Running as root, secrets in layers, unpinned images |

### Pipeline Processing Security
- **[MUST]** Validate and sanitize all incoming data before processing
- **[MUST]** Enforce data isolation between pipelines and tenants
- **[MUST]** Set resource limits (CPU, memory) on pipeline vertices
- **[MUST]** Never log sensitive data flowing through pipelines
- **[SHOULD]** Implement backpressure to prevent resource exhaustion
- **[REDLINE]** Processing untrusted data without validation

### API/Server Security
- **[MUST]** Enforce authentication on all API endpoints
- **[MUST]** Validate all request inputs
- **[MUST]** Return generic error messages; log details server-side only
- **[REDLINE]** Bypassing auth middleware

### K8s/Container Security
- **[MUST]** `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `drop: ["ALL"]`
- **[MUST]** RBAC limited to minimum required permissions
- **[MUST]** Resource limits set on all containers
- **[REDLINE]** `privileged: true`, cluster-admin, wildcard RBAC

### Dockerfile Security
- **[MUST]** Pin base images; multi-stage builds; run as non-root
- **[REDLINE]** Secrets in image layers

### Security Review Format
```txt
🔒 SECURITY REVIEW
Issue: [description]
Severity: CRITICAL | HIGH | MEDIUM | LOW
Location: [file:line]
Recommended Fix: [concrete fix]
```

### Severity Rules
| Severity | Criteria | Action |
|----------|----------|--------|
| **CRITICAL** | Data leakage, credential exposure, auth bypass | **Block** |
| **HIGH** | Missing input validation, excessive RBAC, running as root | **Block** |
| **MEDIUM** | Unpinned deps, missing resource limits | **Flag** |
| **LOW** | Best practice gaps | **Note** |

### Security Checklist
- [ ] No secrets in code/config/logs
- [ ] Pipeline inputs validated and sanitized
- [ ] Data isolation between pipelines enforced
- [ ] Auth enforced on all API endpoints
- [ ] Resource limits set on all containers/vertices
- [ ] Container non-root with minimal capabilities
- [ ] Dependencies and images pinned
- [ ] SCA scanning (Snyk) configured

## Version History
- **v4.0 (2026-02-26):** Initial AGENTS.md with security section for data processing pipelines.
